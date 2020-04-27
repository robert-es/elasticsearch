/*
 * MergeOp.java
 * Copyright 2020 Qunhe Tech, all rights reserved.
 * Qunhe PROPRIETARY/CONFIDENTIAL, any form of usage is subject to approval.
 */

package com.qunhe.es.plugins.constant;

import java.util.Locale;

/**
 * Function: ${Description}
 *
 * @author wuxiang
 * @date 2020/4/21
 */
public enum MergeOp {
    MIN,
    AVG,
    MAX,
    SUM,
    FIRST_MATCH;

    public static MergeOp fromString(String mergeOp) {
        if (null == mergeOp || mergeOp.length() == 0) {
            return FIRST_MATCH;
        }
        return valueOf(mergeOp.toUpperCase(Locale.ROOT));
    }

    public static MergeOp fromObject(Object mergeOp) {
        if (null == mergeOp) {
            return FIRST_MATCH;
        }
        return fromString((String) mergeOp);
    }
}