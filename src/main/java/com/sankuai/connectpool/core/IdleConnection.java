package com.sankuai.connectpool.core;

import java.sql.Connection;

public class IdleConnection {
    private Connection connection;
    private Long addTimeSecond;

    public IdleConnection(Connection connection, Long addTimeSecond) {
        this.connection = connection;
        this.addTimeSecond = addTimeSecond;
    }

    public Connection getConnection() {
        return connection;
    }

    public Long getAddTimeSecond() {
        return addTimeSecond;
    }
}
