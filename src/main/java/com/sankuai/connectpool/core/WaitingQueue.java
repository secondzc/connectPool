package com.sankuai.connectpool.core;

import com.sankuai.connectpool.core.result.ConnectionResult;
import com.sankuai.connectpool.core.result.TimeoutConnectionHolder;

import java.sql.Connection;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

public class WaitingQueue {
    private Queue<TimeoutConnectionHolder> queue = new LinkedBlockingQueue<>();

    /**
     * 入队中并超时等待
     */
    public ConnectionResult inQueueAndGet() {
        TimeoutConnectionHolder timeoutConnectionHolder = new TimeoutConnectionHolder();
        queue.add(timeoutConnectionHolder);
        return timeoutConnectionHolder;
    }

    /**
     * 塞入可用的连接
     * 返回false表示塞入失败，没有任何等待连接
     */
    public boolean offer(Connection connection) {
        Iterator<TimeoutConnectionHolder> iterator = queue.iterator();
        while (iterator.hasNext()) {
            TimeoutConnectionHolder timeoutConnectionHolder = iterator.next();
            if (!timeoutConnectionHolder.getVisited()) {
                timeoutConnectionHolder.offer(connection);
                // double-check一下，避免在还没塞入的这小段时间内已经超时
                if (!timeoutConnectionHolder.getVisited()) {
                    return true;
                }
            }
            iterator.remove();
        }
        return false;
    }
}
