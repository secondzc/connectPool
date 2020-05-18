package com.sankuai.connectpool.core.result;

import java.sql.Connection;

public class ImmediateConnectionResult implements ConnectionResult {
    private Connection connection;

    public ImmediateConnectionResult(Connection connection) {
        this.connection = connection;
    }

    @Override
    public Connection get(int timeoutMs) {
        return connection;
    }
}
