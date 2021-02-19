/*
 * AlreadyClosedException.java
 * Copyright 2021 Qunhe Tech, all rights reserved.
 * Qunhe PROPRIETARY/CONFIDENTIAL, any form of usage is subject to approval.
 */

package com.qunhe.es.store.exceptions;

/**
 * Function: ${Description}
 *
 * @author wuxiang
 * @date 2021/2/8
 */
public class AlreadyClosedException extends IllegalStateException {
    public AlreadyClosedException(String message) {
        super(message);
    }

    public AlreadyClosedException(String message, Throwable cause) {
        super(message, cause);
    }
}
