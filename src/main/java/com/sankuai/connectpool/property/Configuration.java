package com.sankuai.connectpool.property;

import com.sankuai.connectpool.constant.ConfigConst;
import com.sankuai.connectpool.util.PropsUtil;

import java.util.Properties;

public class Configuration {
    private static final Properties props = PropsUtil.loadProps("application.yml");

    public static Integer getCoreConnectNum() {
        return PropsUtil.getInt(props, ConfigConst.coreConnectNum, 2);
    }

    public static Integer getMaxConnectNum() {
        return PropsUtil.getInt(props, ConfigConst.maxConnectNum, 5);
    }

    public static Integer getIdleTimeoutMinute() {
        return PropsUtil.getInt(props, ConfigConst.idleTimeoutMinute, 10);
    }

    public static Integer getWaitTimeoutSecond() {
        return PropsUtil.getInt(props, ConfigConst.waitTimeoutMs, 10);
    }

    public static String getUserName() {
        return PropsUtil.getString(props, ConfigConst.userName, "");
    }

    public static String getPassword() {
        return PropsUtil.getString(props, ConfigConst.password, "");
    }

    public static String getUrl() {
        return PropsUtil.getString(props, ConfigConst.url, "");
    }
}
