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
    FLOAT;

    public static FieldType fromString(String fieldType) {
        if (null == fieldType || fieldType.length() == 0) {
            return LONG;
        }
        return valueOf(fieldType.toUpperCase(Locale.ROOT));
    }
}
