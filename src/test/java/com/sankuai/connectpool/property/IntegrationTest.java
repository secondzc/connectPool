package com.sankuai.connectpool.property;

import com.sankuai.connectpool.core.DataSource;
import com.sankuai.connectpool.core.DataSourceFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;

public class IntegrationTest {
    DataSource dataSource = null;

    @Before
    public void setUp() throws Exception {
        dataSource = DataSourceFactory.getSingleton();
    }

    @Test
    public void testPool() throws Exception{
        int maxConnectNum = Configuration.getMaxConnectNum();
        final CountDownLatch countDownLatch = new CountDownLatch(maxConnectNum + 2);
        for (int i = 0; i < maxConnectNum + 2; i++) {
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    Connection connection = dataSource.acquire();
                    if (connection == null) {
                        System.out.println("acquire connection fail");
                    } else {
                        System.out.println("acquire connection success");
                    }
                    try {
                        // 将sleep时间跳短同时将waitTimeoutMs调长可验证连接池回收连接再利用情况
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    dataSource.release(connection);
                    countDownLatch.countDown();
                }
            }, "test-thread-" + i);
            thread.start();
        }
        countDownLatch.await();
    }
}
