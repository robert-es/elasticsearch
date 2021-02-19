/*
 * BaseDirectory.java
 * Copyright 2021 Qunhe Tech, all rights reserved.
 * Qunhe PROPRIETARY/CONFIDENTIAL, any form of usage is subject to approval.
 */

package com.qunhe.es.bak;

import com.qunhe.es.store.exceptions.AlreadyClosedException;

import java.io.IOException;

/**
 * Function: ${Description}
 *
 * @author wuxiang
 * @date 2021/2/8
 */
public abstract class BaseDirectory extends Directory {
    volatile protected boolean isOpen = true;

    /** Holds the LockFactory instance (implements locking for
     * this Directory instance). */
    protected final LockFactory lockFactory;

    /** Sole constructor. */
    protected BaseDirectory(LockFactory lockFactory) {
        super();
        if (lockFactory == null) {
            throw new NullPointerException(
                    "LockFactory must not be null, use an explicit instance!");
        }
        this.lockFactory = lockFactory;
    }

    @Override
    public final Lock obtainLock(String name) throws IOException {
        return lockFactory.obtainLock(this, name);
    }

    @Override
    protected final void ensureOpen() throws AlreadyClosedException {
        if (!isOpen) {
            throw new AlreadyClosedException("this Directory is closed");
        }
    }

    @Override
    public String toString() {
        return super.toString() + " lockFactory=" + lockFactory;
    }
}
