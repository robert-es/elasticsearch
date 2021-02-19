/*
 * LockFactory.java
 * Copyright 2021 Qunhe Tech, all rights reserved.
 * Qunhe PROPRIETARY/CONFIDENTIAL, any form of usage is subject to approval.
 */

package com.qunhe.es.bak;

import com.qunhe.es.store.exceptions.LockObtainFailedException;

import java.io.IOException;

/**
 * Function: ${Description}
 *
 * @author wuxiang
 * @date 2021/2/8
 */
public abstract class LockFactory {

    /**
     * 创建一个用lockName标记的锁
     * @param lockName 锁名称
     * @throws LockObtainFailedException (optional specific exception) if the lock could
     *         not be obtained because it is currently held elsewhere.
     * @throws IOException if any i/o error occurs attempting to gain the lock
     */
    public abstract Lock obtainLock(Directory dir, String lockName) throws IOException;
}
