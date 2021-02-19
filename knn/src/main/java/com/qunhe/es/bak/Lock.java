/*
 * Lock.java
 * Copyright 2021 Qunhe Tech, all rights reserved.
 * Qunhe PROPRIETARY/CONFIDENTIAL, any form of usage is subject to approval.
 */

package com.qunhe.es.bak;

import org.apache.lucene.store.LockReleaseFailedException;

import java.io.Closeable;
import java.io.IOException;

/**
 * Function: ${Description}
 *
 * @author wuxiang
 * @date 2021/2/8
 */
public abstract class Lock implements Closeable {

    /**
     * 释放锁
     * @throws IOException
     */
    public abstract void close() throws IOException;

    /**
     * 确保锁是合法的，某些情况下，可能锁是非法的，比如认为的删除锁文件
     * @throws IOException
     */
    public abstract void ensureValid() throws IOException;
}