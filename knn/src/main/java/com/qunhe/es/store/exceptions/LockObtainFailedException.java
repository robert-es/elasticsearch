/*
 * LockObtainFailedException.java
 * Copyright 2021 Qunhe Tech, all rights reserved.
 * Qunhe PROPRIETARY/CONFIDENTIAL, any form of usage is subject to approval.
 */

package com.qunhe.es.store.exceptions;

import java.io.IOException;

/**
 * Function: ${Description}
 *
 * @author wuxiang
 * @date 2021/2/8
 */
public class LockObtainFailedException extends IOException {
    public LockObtainFailedException(String message) {
        super(message);
    }

    public LockObtainFailedException(String message, Throwable cause) {
        super(message, cause);
    }

}
