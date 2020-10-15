/*
 * TagMatchScoreUtil.java
 * Copyright 2020 Qunhe Tech, all rights reserved.
 * Qunhe PROPRIETARY/CONFIDENTIAL, any form of usage is subject to approval.
 */

package com.qunhe.es.plugins.util;

import com.qunhe.es.plugins.constant.KvOp;
import com.qunhe.es.plugins.constant.MergeOp;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.qunhe.es.plugins.constant.Const.DEFAULT_SCORE;
import static com.qunhe.es.plugins.constant.Const.DOC_FIELD_TYPE;
import static com.qunhe.es.plugins.constant.Const.KEYS;
import static com.qunhe.es.plugins.constant.Const.SPLIT_SPACE;
import static com.qunhe.es.plugins.constant.Const.VALUES;

/**
 * Function: ${Description}
 *
 * @author wuxiang
 * @date 2020/4/21
 */
public class TagMatctUtil {
    private static final Logger LOG = Loggers.getLogger(TagMatctUtil.class, "[tag-match]");

    private final static int ZERO = 0;
    private final static String STRING_TYPE = "string";
    private final static String LONG_TYPE = "long";
    private final static String INTEGER_TYPE = "integer";

    public static float score(final Map<String, Float> kvPairs, List<Object> fieldValues,
        List<Float> fieldScores, final KvOp kvOp, final MergeOp mergeOp) {
        if (null == kvPairs || kvPairs.size() == ZERO) {
            return DEFAULT_SCORE;
        }

        if (CollectionUtils.isEmpty(fieldValues)) {
            return DEFAULT_SCORE;
        }

        List<Float> scores = new ArrayList<>();
        int length = fieldValues.size();
        Object filedValue;
        for (int i = 0; i < length; i++) {
            filedValue = fieldValues.get(i);
            final String key = filedValue.toString();
            if (!kvPairs.containsKey(key)) {
                continue;
            }

            float score = kvPairs.get(key);
            switch (kvOp) {
            case DOC_VALUE:
                score = fieldScores.get(i);
                break;
            case SUM:
                score += fieldScores.get(i);
                break;
            case AVG:
                score = (score + fieldScores.get(i)) / 2;
                break;
            case MAX:
                score = Math.max(score, fieldScores.get(i));
                break;
            case MIN:
                score = Math.min(score, fieldScores.get(i));
                break;
            case MUL:
                score *= fieldScores.get(i);
                break;
            case QUERY_VALUE:
            default:
                break;
            }
            scores.add(score);
        }

        float score = DEFAULT_SCORE;
        if (CollectionUtils.isNotEmpty(scores)) {
            switch (mergeOp) {
            case MIN:
                score = scores.stream().min(Float::compareTo).get();
                break;
            case MAX:
                score = scores.stream().max(Float::compareTo).get();
                break;
            case AVG:
                score = scores.stream().reduce(DEFAULT_SCORE, (a, b) -> a + b);
                score = score / scores.size();
                break;
            case FIRST_MATCH:
                score = scores.get(ZERO);
                break;
            case SUM:
                score = scores.stream().reduce(DEFAULT_SCORE, (a, b) -> a + b);
                break;
            }
        }

        return score;
    }

    public static Object parseKey(Object object, String fieldType) {
        if (STRING_TYPE.equals(fieldType)) {
            return object.toString();
        } else if (LONG_TYPE.equals(fieldType)) {
            return Long.valueOf(object.toString());
        } else if (INTEGER_TYPE.equals(fieldType)) {
            return Integer.valueOf(object.toString());
        } else {
            throw new IllegalArgumentException(
                "unsupported type of parameter [" + DOC_FIELD_TYPE + "]");
        }
    }

    public static Map<String, Float> parseKvPairs(final KvOp kvOp, final List<?> keys,
        final List<?> values) {
        Map<String, Float> kvPairs = new HashMap<>();
        if (CollectionUtils.isEmpty(values)) {
            throw new IllegalArgumentException("parameter [" + VALUES +
                "] is empty!");
        }

        if (keys.size() != values.size()) {
            throw new IllegalArgumentException(
                "parameter [" + VALUES + "] size must equals " +
                    "to parameter [" + KEYS + "] size");
        }

        float value = DEFAULT_SCORE;
        for (int i = 0; i < keys.size(); i++) {
            switch (kvOp) {
            case QUERY_VALUE:
            case MUL:
            case MIN:
            case MAX:
            case AVG:
            case SUM:
                value = parseFloat(values.get(i));
                break;
            }
            kvPairs.put(keys.get(i).toString(), value);
//                query.kvPairs.put(parseKey(keys.get(i), query.fieldType).toString(), value);
        }

        return kvPairs;
    }

    /**
     * script score 的时候，values list 里面支持混合类型，es会自动转，这个有点恶心
     * List[Double, Integer, Float] 这样子
     * @param object
     * @return
     */
    public static Float parseFloat(Object object) {
        if (object instanceof Float) {
            return (Float) object;
        } else if (object instanceof Double) {
            return ((Double) object).floatValue();
        } else if (object instanceof Integer) {
            return ((Integer) object).floatValue();
        } else {
            throw new IllegalArgumentException(
                "unsupported type of parameter [" + DOC_FIELD_TYPE + "]");
        }
    }

    /**
     * 解析fieldType: STRING_LIST 字段
     */
    public static void parseStringListField(final String field, final String fieldValue,
        final List<Object> fieldValues, final List<Float> fieldScores) {
        String[] valueAndScores = fieldValue.split(SPLIT_SPACE);
        if (!(valueAndScores.length % 2 == 0)) {
            LOG.error(field + "字段值错误，字段切割后，数量不是偶数!");
            return;
        }

        for (int i = 0; i < valueAndScores.length; i += 2) {
            fieldValues.add(valueAndScores[i].trim());
            Float fieldScore = Float.parseFloat(valueAndScores[i + 1].trim());
            fieldScores.add(fieldScore);
        }
    }
}
