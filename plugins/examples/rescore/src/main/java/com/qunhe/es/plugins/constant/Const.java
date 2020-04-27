/*
 * Const.java
 * Copyright 2020 Qunhe Tech, all rights reserved.
 * Qunhe PROPRIETARY/CONFIDENTIAL, any form of usage is subject to approval.
 */

package com.qunhe.es.plugins.constant;

import org.elasticsearch.common.ParseField;

/**
 * Function: ${Description}
 *
 * @author wuxiang
 * @date 2020/4/21
 */
public class Const {
    // field 和 value 储存成字符串
//    public final static String LIST_STR_TYPE = "string_list";

    public final static String SPLIT_SPACE = ",";

    public static final String DOC_FIELD_TYPE = "field_type";
    public static final float DEFAULT_SCORE = 0f;

    public static final ParseField FIELD = new ParseField("field");
    public static final ParseField SCORE_FIELD = new ParseField("score_field");
    public static final ParseField FIELD_TYPE = new ParseField("field_type");
    public static final ParseField KV_OP = new ParseField("kv_op");
    public static final ParseField MERGE_OP = new ParseField("merge_op");
    public static final ParseField KEYS = new ParseField("keys");
    // 参数中字段值对应的分数
    public static final ParseField VALUES = new ParseField("values");
}
