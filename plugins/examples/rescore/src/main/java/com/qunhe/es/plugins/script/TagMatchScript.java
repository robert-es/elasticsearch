/*
 * TagMatchScript.java
 * Copyright 2020 Qunhe Tech, all rights reserved.
 * Qunhe PROPRIETARY/CONFIDENTIAL, any form of usage is subject to approval.
 */

package com.qunhe.es.plugins.script;

import com.qunhe.es.plugins.constant.FieldType;
import com.qunhe.es.plugins.constant.KvOp;
import com.qunhe.es.plugins.constant.MergeOp;
import com.qunhe.es.plugins.util.CollectionUtils;
import com.qunhe.es.plugins.util.TagMatctUtil;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.lookup.SourceLookup;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.qunhe.es.plugins.constant.Const.DEFAULT_SCORE;
import static com.qunhe.es.plugins.constant.Const.DOC_FIELD_TYPE;
import static com.qunhe.es.plugins.constant.Const.FIELD_TYPE;
import static com.qunhe.es.plugins.constant.Const.SPLIT_SPACE;
import static com.qunhe.es.plugins.util.LuceneUtil.getPayloadFromIndex;
import static com.qunhe.es.plugins.util.TagMatctUtil.parseFloat;
import static com.qunhe.es.plugins.util.TagMatctUtil.parseKvPairs;
import static com.qunhe.es.plugins.util.TagMatctUtil.parseStringListField;

/**
 * Function: ${Description}
 *
 * @author wuxiang
 * @date 2020/4/17
 */
public class TagMatchScript extends ScoreScript {
    private static final Logger LOG = Loggers.getLogger(TagMatchScript.class, "[tag-match]");

    public static final String DOC_FIELD = "field";
    /**
     * 匹配的字段值
     */
    public static final String KEYS = "keys";
    /**
     * 字段值对应的value
     */
    public static final String VALUES = "values";

    /**
     * 参数KEY、VALUE 如何与doc 对应的字段进行组合分数
     */
    public static final String KV_OP = "kv_op";
    /**
     * 匹配多个key，key之间如何组合分数
     */
    public static final String MERGE_OP = "merge_op";
    private final static int ZERO = 0;


    private String field;
    private FieldType fieldType;
    private KvOp kvOp;
    private MergeOp mergeOp;
    final Map<String, Object> params;
    Map<String, Float> kvPairs;
    private final LeafReaderContext context;
    private final SearchLookup lookup;


    public TagMatchScript(final Map<String, Object> params, final SearchLookup lookup,
        final LeafReaderContext leafContext) {
        super(params, lookup, leafContext);
        this.params = params;
        this.context = leafContext;
        this.lookup = lookup;
    }

    @Override
    public double execute() {
        parseParams();

        if (null == this.kvPairs || this.kvPairs.size() == ZERO) {
            return DEFAULT_SCORE;
        }

        List<Float> fieldScores = null;
        List<Object> fieldValues = null;
        if (FieldType.PAYLOAD.equals(this.fieldType)) {
            try {
                fieldScores = new ArrayList<>();
                fieldValues = new ArrayList<>();
                getPayloadFromIndex(context.reader(), this.field, fieldValues, fieldScores);
            } catch (Exception e) {
                LOG.error(this.field + " 获取去payload失败!" + e.getMessage());
                return DEFAULT_SCORE;
            }
        } else if (FieldType.STRING_LIST.equals(this.fieldType)) {
            ScriptDocValues scriptDocValues = getDoc().get(field);
            if (null == scriptDocValues || scriptDocValues.getValues().size() == 0) {
                return DEFAULT_SCORE;
            }

            fieldScores = new ArrayList<>();
            fieldValues = new ArrayList<>();
            String fieldValue = (String) scriptDocValues.getValues().get(0);
            parseStringListField(this.field, fieldValue, fieldValues, fieldScores);

        } else if (FieldType.LIST_WITH_SCORE.equals(this.fieldType)) {
            SourceLookup sourceLookup = lookup.getLeafSearchLookup(context).source();
            List values = (List) sourceLookup.get(this.field);
            int size = values.size();
            if ((size % 2) != 0) {
                LOG.error(field + "list数组大小不是偶数!");
                return DEFAULT_SCORE;
            }

            fieldScores = new ArrayList<>();
            fieldValues = new ArrayList<>();
            for (int i = 0; i < size; i += 2) {
                fieldValues.add(values.get(i));
                fieldScores.add(parseFloat(values.get(i + 1)));
            }
        } else {
            ScriptDocValues scriptDocValues = getDoc().get(field);
            fieldValues = scriptDocValues.getValues();
        }

        return TagMatctUtil.score(kvPairs, fieldValues, fieldScores, kvOp, mergeOp);
    }

    private void parseParams() {
        //所有是否包含需要的字段校验逻辑都在TagMatchScriptFactory做了，这里不重复
        this.field = (String) this.params.get(DOC_FIELD);
        this.fieldType = FieldType.fromString((String) this.params.get(DOC_FIELD_TYPE));
        List keys = (List) this.params.get(KEYS);
        List<Float> values = null;
        if (null != this.params.get(VALUES)) {
            values = (List<Float>) this.params.get(VALUES);
        }
        this.kvOp = KvOp.fromObject(this.params.get(KV_OP));
        this.mergeOp = MergeOp.fromObject(this.params.get(MERGE_OP));


        if (CollectionUtils.isNotEmpty(keys)) {
            kvPairs = parseKvPairs(this.kvOp, keys, values);
        }

        if (!FieldType.STRING_LIST.equals(this.fieldType) && !FieldType.LIST_WITH_SCORE.equals
            (this.fieldType) && !FieldType.PAYLOAD.equals(this.fieldType)) {
            switch (this.kvOp) {
            case DOC_VALUE:
            case MIN:
            case AVG:
            case MAX:
            case SUM:
            case MUL:
                throw new IllegalArgumentException(
                    "parameter [" + FIELD_TYPE + "] error!");
            }
        }
    }


    public static void main(String[] args) {
        List<Double> scores = new ArrayList<>(Arrays.asList(19d, 9d, 32d, 21d, 2d, 17d));
        System.out.println(scores.stream().max(Double::compareTo).get());
        System.out.println(scores.stream().min(Double::compareTo).get());
        Double score = scores.stream().reduce(0d, (a, b) -> a + b);
        score = score / scores.size();
        System.out.println(score);

    }
}
