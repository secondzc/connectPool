package com.sankuai.connectpool.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class WaitingQueue {
    private Queue<ConnectionHolder> queue = new LinkedBlockingQueue<>();

    /**
     * 入队中并超时等待
     */
    public Connection inQueueAndGet(int timeout) {
        ConnectionHolder connectionHolder = new ConnectionHolder();
        queue.add(connectionHolder);
        return connectionHolder.get(timeout);
    }

    /**
     * 塞入可用的连接
     * 返回false表示塞入失败，没有任何等待连接
     */
    public boolean offer(Connection connection) {
        Iterator<ConnectionHolder> iterator = queue.iterator();
        while (iterator.hasNext()) {
            ConnectionHolder connectionHolder = iterator.next();
            if (!connectionHolder.getVisited()) {
                connectionHolder.offer(connection);
                // double-check一下，避免在还没塞入的这小段时间内已经超时
                if (!connectionHolder.getVisited()) {
                    return true;
                }
            }
            iterator.remove();
        }
        return false;
    }

    public class ConnectionHolder {
        private final Logger LOGGER = LoggerFactory.getLogger(ConnectionHolder.class);

        private BlockingQueue<Connection> connectionResultHolder = new ArrayBlockingQueue<>(1);
        private volatile boolean visited = false; //true表示已经拿到connection，或者超时还没拿到，都不用往里面再塞connection

        public Connection get(int timeoutMs) {
            Connection connection = null;
            try {
                connection = connectionResultHolder.poll(timeoutMs, TimeUnit.MILLISECONDS);
                visited = true;
            } catch (InterruptedException e) {
                return null;
            }
            if (connection == null) {
                return null;
            }
            return connection;
        }

        public boolean getVisited() {
            return visited;
        }

        public void offer(Connection connection) {
            try {
                connectionResultHolder.offer(connection);
            } catch (Exception e) {
                LOGGER.error("connectionWrapper connectionResultHolder fail to offer element");
            }
        }
    }
}
