/*
 * KvOp.java
 * Copyright 2020 Qunhe Tech, all rights reserved.
 * Qunhe PROPRIETARY/CONFIDENTIAL, any form of usage is subject to approval.
 */

package com.qunhe.es.plugins.constant;

/**
 * Function: ${Description}
 *
 * @author wuxiang
 * @date 2020/4/21
 */

import java.util.Locale;

/**
 * 这里不支持 opensearch中max、min、sum、avg、mul等功能，如果后续有需要在考虑扩展
 */
public enum KvOp {
    /**
     * 分数为 values参数里面的取值
     */
    QUERY_VALUE,
    DOC_VALUE,
    MIN,
    AVG,
    MAX,
    SUM,
    MUL;

    public static KvOp fromString(String kvOp) {
        if (null == kvOp || kvOp.length() == 0) {
            return QUERY_VALUE;
        }
        return valueOf(kvOp.toUpperCase(Locale.ROOT));
    }

    public static KvOp fromObject(Object kvOp) {
        if (null == kvOp) {
            return QUERY_VALUE;
        }
        return fromString((String) kvOp);
    }
}

