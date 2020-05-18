package com.sankuai.connectpool.core.result;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class TimeoutConnectionHolder implements ConnectionResult {
    private final Logger LOGGER = LoggerFactory.getLogger(TimeoutConnectionHolder.class);

    private BlockingQueue<Connection> connectionResultHolder = new ArrayBlockingQueue<>(1);
    private volatile boolean visited = false; //true表示已经拿到connection，或者超时还没拿到，都不用往里面再塞connection

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

    @Override
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
}
