/*
 * TagMatchQueryBuilder.java
 * Copyright 2020 Qunhe Tech, all rights reserved.
 * Qunhe PROPRIETARY/CONFIDENTIAL, any form of usage is subject to approval.
 */

package com.qunhe.es.plugins.query;

import com.qunhe.es.plugins.constant.KvOp;
import com.qunhe.es.plugins.constant.MergeOp;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.List;

import static com.qunhe.es.plugins.constant.Const.FIELD;
import static com.qunhe.es.plugins.constant.Const.FIELD_TYPE;
import static com.qunhe.es.plugins.constant.Const.KEYS;
import static com.qunhe.es.plugins.constant.Const.KV_OP;
import static com.qunhe.es.plugins.constant.Const.MERGE_OP;
import static com.qunhe.es.plugins.constant.Const.SCORE_FIELD;
import static com.qunhe.es.plugins.constant.Const.VALUES;
import static com.qunhe.es.plugins.constant.KvOp.AVG;
import static com.qunhe.es.plugins.constant.KvOp.DOC_VALUE;
import static com.qunhe.es.plugins.constant.KvOp.MAX;
import static com.qunhe.es.plugins.constant.KvOp.MIN;
import static com.qunhe.es.plugins.constant.KvOp.MUL;
import static com.qunhe.es.plugins.constant.KvOp.SUM;
import static com.qunhe.es.plugins.query.TagMatchQuery.LIST_STR_TYPE;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Function: ${Description}
 *
 * @author wuxiang
 * @date 2020/4/22
 */
public class TagMatchQueryBuilder extends AbstractQueryBuilder<TagMatchQueryBuilder> {
    public final static String NAME = "tag-match";

    private List<Long> keys;
    private List<Float> values;
    private String field;
    private String scoreField;
    private String fieldType;
    private KvOp kvOp;
    private MergeOp mergeOp;

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<TagMatchQueryBuilder, Void> PARSER =
        new ConstructingObjectParser<>(NAME,
            args -> new TagMatchQueryBuilder((String) args[0], (String) args[1],
                (String) args[2],
                (String) args[3], (String) args[4], (List<Long>) args[5],
                (List<Float>) args[6]));

    static {
        PARSER.declareString(constructorArg(), FIELD);
        PARSER.declareString(optionalConstructorArg(), SCORE_FIELD);
        PARSER.declareString(optionalConstructorArg(), FIELD_TYPE);
        PARSER.declareString(constructorArg(), KV_OP);
        PARSER.declareString(constructorArg(), MERGE_OP);
        PARSER.declareLongArray(constructorArg(), KEYS);
        PARSER.declareFloatArray(constructorArg(), VALUES);
    }


    public TagMatchQueryBuilder(String field, String scoreField, String fieldType, String kvOp,
        String mergeOp, List<Long> keys, List<Float> values) {
        this.field = field;
        this.scoreField = scoreField;
        this.fieldType = fieldType;
        this.kvOp = KvOp.fromString(kvOp);
        this.mergeOp = MergeOp.fromString(mergeOp);
        this.keys = keys;
        this.values = values;

        if (!LIST_STR_TYPE.equals(this.fieldType)) {
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

    public TagMatchQueryBuilder(StreamInput in) throws IOException {
        super(in);
    }

    /**
     * 根据输入的文本，解析成对应的对象
     * @param parser
     * @return
     */
    public static TagMatchQueryBuilder fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    protected void doWriteTo(final StreamOutput streamOutput) throws IOException {

    }

    @Override
    protected void doXContent(final XContentBuilder xContentBuilder, final Params params)
        throws IOException {

    }

    @Override
    protected TagMatchQuery doToQuery(final QueryShardContext queryShardContext)
        throws IOException {
        return TagMatchQuery.build(field, scoreField, fieldType, kvOp, mergeOp, keys, values);
    }

    @Override
    protected boolean doEquals(final TagMatchQueryBuilder tagMatchQueryBuilder) {
        return false;
    }

    @Override
    protected int doHashCode() {
        return 0;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
