package com.sankuai.connectpool.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class ConnectionWrapper {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionWrapper.class);

    private BlockingQueue<Connection> queue = new ArrayBlockingQueue<>(1);

    public Connection get(int timeoutMs) {
        Connection connection = null;
        try {
            connection = queue.poll(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            // 超时返回空
            return null;
        }
        return connection;
    }

    public void offer(Connection connection) {
        try {
            queue.offer(connection);
        } catch (Exception e) {
            LOGGER.error("connectionWrapper queue fail to offer element");
        }
    }
}
