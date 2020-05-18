package com.sankuai.connectpool.core;

import com.sankuai.connectpool.exception.ArgumentException;
import com.sankuai.connectpool.property.Configuration;
import com.sankuai.connectpool.util.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

// TODO: 2020-05-18 如果连接长时间没有数据库操作，应该监控并回收
// TODO: 2020-05-18 因为并发条件下activeConnectionList的size大小问题，暂时将acquire和release两个都加锁了，粒度较大，
//  且这样一来都不需要使用并发容器了。看是否有啥其他解决方案。
public class DataSource {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataSource.class);

    private int coreConnectNum = 0;
    private int maxConnectNum = 0;
    private int ideTimeoutMinute = 0;
    private int waitTimeoutSecond = 0;

    private List<Connection> activeConnectionList = Collections.synchronizedList(new ArrayList<Connection>());
    private Queue<IdleConnection> idleConnectionQueue = new ConcurrentLinkedQueue<>();
    private WaitingQueue waitingQueue = new WaitingQueue();
    private ScheduledExecutorService clearIdleConnectionScheduler = Executors.newScheduledThreadPool(1);

    {
        /**
         * 定时任务用来清理过期的连接
         */
        clearIdleConnectionScheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    Iterator<IdleConnection> iterator = idleConnectionQueue.iterator();
                    while (iterator.hasNext()) {
                        IdleConnection idleConnection = iterator.next();
                        long curTimeSecond = DateUtil.getCurrentTimeSecond();
                        if (curTimeSecond < ideTimeoutMinute * 60 + idleConnection.getAddTimeSecond()) {
                            continue;
                        }
                        Connection connection = idleConnection.getConnection();
                        doDisConnect(connection);
                        LOGGER.info("idle connection cleared");
                        iterator.remove();
                    }
                } catch (Exception e) {
                    LOGGER.error("clear idle connection scheduler error", e);
                }
            }
        }, 0, 1, TimeUnit.SECONDS);
    }

    public DataSource(int coreConnectNum, int maxConnectNum, int ideTimeoutMinute, int waitTimeoutSecond) {
        checkParams(coreConnectNum, maxConnectNum, ideTimeoutMinute, waitTimeoutSecond);
        this.coreConnectNum = coreConnectNum;
        this.maxConnectNum = maxConnectNum;
        this.ideTimeoutMinute = ideTimeoutMinute;
        this.waitTimeoutSecond = waitTimeoutSecond;
    }

    /**
     * 活跃连接数+空闲线连接数：
     * 小于coreConnectNum时，建立新连接
     * 大于等于maxConnectNum时，有空闲连接则使用空闲连接，没有空闲连接也不创建，超时等待活跃连接释放
     * 大于coreConnectNum且小于maxConnectNum时，有空闲连接则使用空闲连接，如果没有空闲连接，则创建新连接
     */
    public synchronized Connection acquire() {
        int activeConnectNum = activeConnectionList.size();
        int idleConnectNum = idleConnectionQueue.size();
        if (activeConnectNum + idleConnectNum < coreConnectNum) {
            Connection connection = doConnect();
            activeConnectionList.add(connection);
            LOGGER.info("dataSource acquire, [1]build new connect success, activeConnectNum:{}, idleConnectNum:{}", activeConnectNum, idleConnectNum);
            return connection;
        }

        if (activeConnectNum + idleConnectNum >= maxConnectNum) {
            IdleConnection idleConnection = idleConnectionQueue.poll();
            if (idleConnection != null) {
                LOGGER.info("dataSource acquire, [2]use idle connect success, activeConnectNum:{}, idleConnectNum:{}", activeConnectNum, idleConnectNum);
                return idleConnection.getConnection();
            }
            if (waitTimeoutSecond <= 0) {
                LOGGER.info("dataSource acquire, [3]no timeout fail, activeConnectNum:{}, idleConnectNum:{}", activeConnectNum, idleConnectNum);
                return null;
            }
            Connection connection = waitingQueue.inQueueAndGet(waitTimeoutSecond);
            if (connection == null) {
                LOGGER.info("dataSource acquire, [4]wait idle timeout fail, activeConnectNum:{}, idleConnectNum:{}", activeConnectNum, idleConnectNum);
                return null;
            }
            activeConnectionList.add(connection);
            LOGGER.info("dataSource acquire, [5]wait idle success, activeConnectNum:{}, idleConnectNum:{}", activeConnectNum, idleConnectNum);
            return connection;
        }

        IdleConnection idleConnection = idleConnectionQueue.poll();
        if (idleConnection == null) {
            Connection connection = doConnect();
            activeConnectionList.add(connection);
            LOGGER.info("dataSource acquire, [6]build new connect success, activeConnectNum:{}, idleConnectNum:{}", activeConnectNum, idleConnectNum);
            return connection;
        }
        LOGGER.info("dataSource acquire, [7]use idle connect, activeConnectNum:{}, idleConnectNum:{}", activeConnectNum, idleConnectNum);
        return idleConnection.getConnection();
    }

    /**
     * 优先满足正在等待连接的请求，若没有等待的则放入空闲队列
     */
    public synchronized void release(Connection connection) {
        boolean addToWaitingQueueSuccess = waitingQueue.offer(connection);
        if (addToWaitingQueueSuccess) {
            LOGGER.info("dataSource release, put to waitingQueue");
            return;
        }
        LOGGER.info("dataSource release, put to idleQueue");
        idleConnectionQueue.add(new IdleConnection(connection, DateUtil.getCurrentTimeSecond()));
    }

    private void checkParams(int coreConnectNum, int maxConnectNum, int ideTimeoutMinute, int waitTimeoutSecond) {
        if (coreConnectNum <= 0
                || maxConnectNum <= 0
                || ideTimeoutMinute <= 0
                || waitTimeoutSecond <= 0) {
            throw new ArgumentException("params less than zero");
        }
        if (coreConnectNum > maxConnectNum) {
            throw new ArgumentException("coreConnectNum more than maxConnectNum");
        }
    }

    private Connection doConnect() {
        String userName = Configuration.getUserName();
        String password = Configuration.getPassword();
        String url = Configuration.getUrl();
        Connection connection = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection(url, userName, password);// 获取连接
        } catch (Exception e) {
            throw new RuntimeException("connect fail");
        }
        return connection;
    }

    private void doDisConnect(Connection connection) {
        if (connection == null) {
            return ;
        }
        try {
            connection.close();
        } catch (Exception e) {
            throw new RuntimeException("disconnect fail");
        }
    }
}
