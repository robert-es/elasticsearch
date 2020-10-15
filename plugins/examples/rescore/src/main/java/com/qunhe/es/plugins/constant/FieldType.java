/*
 * FieldType.java
 * Copyright 2020 Qunhe Tech, all rights reserved.
 * Qunhe PROPRIETARY/CONFIDENTIAL, any form of usage is subject to approval.
 */

package com.qunhe.es.plugins.constant;

import java.util.Locale;

/**
 * Function: ${Description}
 *
 * @author wuxiang
 * @date 2020/4/27
 */
public enum FieldType {
    //field 和 score 存储成一个字符串
    STRING_LIST,
    // field 和score 存储成一个double list
    LIST_WITH_SCORE,
    LONG,
    INTEGER,
    DOUBLE,
    FLOAT,
    PAYLOAD;

    public static FieldType fromString(String fieldType) {
        if (null == fieldType || fieldType.length() == 0) {
            throw new IllegalArgumentException("illegal field_type [" + fieldType + "]");
        }
        return valueOf(fieldType.toUpperCase(Locale.ROOT));
    }
}
