package com.sankuai.connectpool.core.result;

import java.sql.Connection;

public interface ConnectionResult {
    Connection get(int timeoutMs);
}
