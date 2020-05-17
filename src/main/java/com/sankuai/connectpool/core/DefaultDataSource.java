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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DefaultDataSource {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultDataSource.class);

    private int coreConnectNum = 0;
    private int maxConnectNum = 0;
    private int ideTimeoutMinute = 0;
    private int waitTimeoutSecond = 0;

    private List<Connection> activeConnectionList = Collections.synchronizedList(new ArrayList<Connection>());
    private Queue<IdleConnection> idleConnectionQueue = new ConcurrentLinkedDeque<>();
    private BlockingQueue<ConnectionWrapper> waitingQueue = new LinkedBlockingQueue<>();
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
                        iterator.remove();
                        Connection connection = idleConnection.getConnection();
                        doDisConnect(connection);
                    }
                } catch (Exception e) {
                    LOGGER.error("clear idle connection scheduler error", e);
                }
            }
        }, 0, 1, TimeUnit.SECONDS);
    }

    public DefaultDataSource(int coreConnectNum, int maxConnectNum, int ideTimeoutMinute, int waitTimeoutSecond) {
        checkParams(coreConnectNum, maxConnectNum, ideTimeoutMinute, waitTimeoutSecond);
        this.coreConnectNum = coreConnectNum;
        this.maxConnectNum = maxConnectNum;
        this.ideTimeoutMinute = ideTimeoutMinute;
        this.waitTimeoutSecond = waitTimeoutSecond;
    }

    /**
     * 小于coreConnectNum时，一律建立新连接
     * 大于coreConnectNum且小于maxConnectNum时，有空闲，则使用空闲连接，如果没有空闲，则创建新连接
     * 大于等于maxConnectNum时，阻塞等待
     */
    public Connection acquire() {
        int activeConnectNum = activeConnectionList.size();

        if (activeConnectNum < coreConnectNum) {
            Connection connection = doConnect();
            activeConnectionList.add(connection);
            return connection;
        }

        if (activeConnectNum >= maxConnectNum) {
            if (waitTimeoutSecond <= 0) {
                return null;
            }
            ConnectionWrapper connectionWrapper = new ConnectionWrapper();
            waitingQueue.add(connectionWrapper);
            return connectionWrapper.get(waitTimeoutSecond);
        }

        IdleConnection idleConnection = idleConnectionQueue.poll();
        if (idleConnection == null) {
            Connection connection = doConnect();
            activeConnectionList.add(connection);
            return connection;
        }
        return idleConnection.getConnection();
    }

    /**
     * 小于coreConnectNum时，放入活跃队列中
     * 大于coreConnectNum时，放入空闲队列中
     */
    public void release(Connection connection) {
        int activeConnectNum = activeConnectionList.size();
        if (activeConnectNum < coreConnectNum) {
            activeConnectionList.add(connection);
            return ;
        }
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
