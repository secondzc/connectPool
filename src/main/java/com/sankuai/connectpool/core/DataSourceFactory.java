package com.sankuai.connectpool.core;

import com.sankuai.connectpool.property.Configuration;

public class DataSourceFactory {
    static class DataSourceHolder {
        private static DataSource dataSource = new DataSource(Configuration.getCoreConnectNum(),
                Configuration.getMaxConnectNum(),
                Configuration.getIdleTimeoutMinute(),
                Configuration.getWaitTimeoutSecond());
    }

    public static DataSource getSingleton() {
        return DataSourceHolder.dataSource;
    }
}
