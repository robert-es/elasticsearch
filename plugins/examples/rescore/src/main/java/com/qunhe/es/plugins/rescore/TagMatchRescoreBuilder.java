/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.qunhe.es.plugins.rescore;


import com.qunhe.es.plugins.constant.KvOp;
import com.qunhe.es.plugins.constant.MergeOp;
import com.qunhe.es.plugins.constant.RescoreMode;
import com.qunhe.es.plugins.util.CollectionUtils;
import com.qunhe.es.plugins.util.TagMatctUtil;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.AtomicNumericFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.rescore.RescoreContext;
import org.elasticsearch.search.rescore.Rescorer;
import org.elasticsearch.search.rescore.RescorerBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.qunhe.es.plugins.constant.Const.DEFAULT_SCORE;
import static com.qunhe.es.plugins.util.TagMatctUtil.parseKey;
import static java.util.Collections.singletonList;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Example rescorer that multiplies the score of the hit by some factor and doesn't resort them.
 */
public class TagMatchRescoreBuilder extends RescorerBuilder<TagMatchRescoreBuilder> {
    public static final String NAME = "tag-match";
    private static final ParseField FIELD = new ParseField("field");
    private static final ParseField FIELD_TYPE = new ParseField("field_type");
    private static final ParseField KV_OP = new ParseField("kv_op");
    private static final ParseField MERGE_OP = new ParseField("merge_op");
    private static final ParseField KEYS = new ParseField("keys");
    private static final ParseField VALUES = new ParseField("values");
    private static final ParseField QUERY_WEIGHT = new ParseField("query_weight");
    private static final ParseField RESCORE_QUERY_WEIGHT = new ParseField("rescore_query_weight");
    private static final ParseField RESCORE_MODE = new ParseField("rescore_mode");


    private final static int ZERO = 0;
    private final static String STRING_TYPE = "string";
    private final static String LONG_TYPE = "long";
    private final static String INTEGER_TYPE = "integer";
    private static final Logger LOG = Loggers.getLogger(TagMatchRescoreBuilder.class,
        "[tag-match don't support list vlaues now ]");


    List<Long> keys;
    List<Float> values;
    private String field;
    private String fieldType;
    private KvOp kvOp;
    private MergeOp mergeOp;
    private Float queryWeight;
    private Float rescoreQueryWeight;
    private RescoreMode rescoreMode;


    public TagMatchRescoreBuilder(String field, String fieldType, String kvOp, String
        mergeOp, List<Long> keys, List<Float> values, Float queryWeight, Float
        rescoreQueryWeight, String rescoreMode) {
        this.field = field;
        this.fieldType = fieldType;
        this.kvOp = KvOp.fromString(kvOp);
        this.mergeOp = MergeOp.fromString(mergeOp);
        this.keys = keys;
        this.values = values;
        this.queryWeight = queryWeight;
        this.rescoreQueryWeight = rescoreQueryWeight;
        this.rescoreMode = RescoreMode.fromString(rescoreMode);
    }

    TagMatchRescoreBuilder(StreamInput in) throws IOException {
        super(in);
    }

    /**
     * TODO: 搞清楚这个干吗用的
     * @param out
     * @throws IOException
     */
    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(field);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public RescorerBuilder<TagMatchRescoreBuilder> rewrite(QueryRewriteContext ctx)
        throws IOException {
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(FIELD.getPreferredName(), field);
    }

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<TagMatchRescoreBuilder, Void> PARSER =
        new ConstructingObjectParser<>(NAME,
            args -> new TagMatchRescoreBuilder((String) args[0], (String) args[1],
                (String) args[2], (String) args[3], (List<Long>) args[4],
                (List<Float>) args[5], (Float) args[6], (Float) args[7], (String)
                args[8]));

    static {
        PARSER.declareString(constructorArg(), FIELD);
        PARSER.declareString(constructorArg(), FIELD_TYPE);
        PARSER.declareString(constructorArg(), KV_OP);
        PARSER.declareString(constructorArg(), MERGE_OP);
        //TODO :如何支持泛型呢？
        PARSER.declareIntArray(constructorArg(), KEYS);
        PARSER.declareFloatArray(constructorArg(), VALUES);
        PARSER.declareFloat(constructorArg(), QUERY_WEIGHT);
        PARSER.declareFloat(constructorArg(), RESCORE_QUERY_WEIGHT);
        PARSER.declareString(constructorArg(), RESCORE_MODE);

    }

    public static TagMatchRescoreBuilder fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public RescoreContext innerBuildContext(int windowSize, QueryShardContext context)
        throws IOException {
        IndexFieldData<?> factorField =
            this.field == null ? null : context.getForField(
                context.fieldMapper(this.field));
        return new TagMatchRescoreContext(windowSize, factorField, field, fieldType, kvOp, mergeOp,
            keys, values, queryWeight, rescoreQueryWeight, rescoreMode);
    }

    @Override
    public boolean equals(Object obj) {
        if (false == super.equals(obj)) {
            return false;
        }
        TagMatchRescoreBuilder other = (TagMatchRescoreBuilder) obj;
        return field == other.field
            && this.fieldType == other.fieldType
            && this.kvOp == other.kvOp
            && this.mergeOp == other.mergeOp
            && Objects.equals(this.keys, other.keys)
            && Objects.equals(this.values, other.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), field, fieldType, kvOp, mergeOp, keys, values);
    }

    public List<Long> getKeys() {
        return keys;
    }

    public List<Float> getValues() {
        return values;
    }

    public String getField() {
        return field;
    }

    public String getFieldType() {
        return fieldType;
    }

    public KvOp getKvOp() {
        return kvOp;
    }

    public MergeOp getMergeOp() {
        return mergeOp;
    }

    private static class TagMatchRescoreContext extends RescoreContext {
        private String field;
        @Nullable
        private final IndexFieldData<?> indexFieldData;
        List<Long> keys;
        List<Float> values;
        private String fieldType;
        private KvOp kvOp;
        private MergeOp mergeOp;
        private Map<String, Float> kvPairs;
        private Float queryWeight;
        private Float rescoreQueryWeight;
        private RescoreMode rescoreMode;


        TagMatchRescoreContext(int windowSize, @Nullable IndexFieldData<?> indexFieldData,
            String field, String fieldType, KvOp kvOp, MergeOp mergeOp, List<Long> keys,
            List<Float> values, Float queryWeight, Float rescoreQueryWeight,
            RescoreMode rescoreMode) {
            super(windowSize, TagMatchRescorer.INSTANCE);
            this.field = field;
            this.indexFieldData = indexFieldData;
            this.fieldType = fieldType;
            this.kvOp = kvOp;
            this.mergeOp = mergeOp;
            this.keys = keys;
            this.values = values;
            this.queryWeight = queryWeight;
            this.rescoreQueryWeight = rescoreQueryWeight;
            this.rescoreMode = rescoreMode;

            if (CollectionUtils.isNotEmpty(keys)) {

                kvPairs = new HashMap<>();
                if (CollectionUtils.isEmpty(values)) {
                    throw new IllegalArgumentException("parameter [" + VALUES + "] is empty!");
                }

                if (keys.size() != values.size()) {
                    throw new IllegalArgumentException(
                        "parameter [" + VALUES + "] size must equals " +
                            "to parameter [" + KEYS + "] size");
                }


                float value = DEFAULT_SCORE;
                for (int i = 0; i < keys.size(); i++) {
                    switch (this.kvOp) {
                    case QUERY_VALUE:
                        value = values.get(i);
                        break;
                    }
                    kvPairs.put(parseKey(keys.get(i), this.fieldType).toString(), value);
                }
            }
        }
    }

    private static class TagMatchRescorer implements Rescorer {
        private static final TagMatchRescorer INSTANCE = new TagMatchRescorer();

        @Override
        public TopDocs rescore(TopDocs topDocs, IndexSearcher searcher,
            RescoreContext rescoreContext) throws IOException {
            TagMatchRescoreContext context = (TagMatchRescoreContext) rescoreContext;
            int end = Math.min(topDocs.scoreDocs.length, rescoreContext.getWindowSize());
            if (end == 0) {
                return topDocs;
            }

            if (context.indexFieldData != null) {
                /*
                 * Since he we looks up a single field value it should
                 * access them in docId order because that is the order in
                 * which they are stored on disk and we want reads to be
                 * forwards and close together if possible.
                 *
                 * If accessing multiple fields we'd be better off accessing
                 * them in (reader, field, docId) order because that is the
                 * order they are on disk.
                 */
                ScoreDoc[] sortedByDocId = new ScoreDoc[topDocs.scoreDocs.length];
                System.arraycopy(topDocs.scoreDocs, 0, sortedByDocId, 0, topDocs.scoreDocs.length);
                Arrays.sort(sortedByDocId, (a, b) -> a.doc - b.doc); // Safe because doc ids >= 0
                Iterator<LeafReaderContext> leaves = searcher.getIndexReader().leaves().iterator();
                LeafReaderContext leaf = null;
                AtomicFieldData fd = null;
                SortedNumericDocValues docValues = null;
                int endDoc = 0;
                for (int i = 0; i < end; i++) {
                    if (topDocs.scoreDocs[i].doc >= endDoc) {
                        do {
                            leaf = leaves.next();
                            endDoc = leaf.docBase + leaf.reader().maxDoc();
                        } while (topDocs.scoreDocs[i].doc >= endDoc);

                        fd = context.indexFieldData.load(leaf);
                        if (false == (fd instanceof AtomicNumericFieldData)) {
                            throw new IllegalArgumentException(
                                "[" + context.indexFieldData.getFieldName() +
                                    "] is not a number");
                        }
                        docValues = ((AtomicNumericFieldData) fd).getLongValues();
//                        docValues = DocValues.getSortedNumeric(leaf.reader(), context.field);
                    }

                    if (docValues.advanceExact(topDocs.scoreDocs[i].doc)) {
                        List<Object> fieldValues = new ArrayList<>();
                        //TODO: 多个值的数组类型不知道为什么不行，后续在研究
                        if (docValues.docValueCount() > 1) {
                            throw new IllegalArgumentException(
                                "document [" + topDocs.scoreDocs[i].doc
                                    + "] has more than one value for [" +
                                    context.indexFieldData.getFieldName() + "]");
//                            LOG.error("document [" + topDocs.scoreDocs[i].doc
//                                    + "] has more than one value for [" +
//                                    context.indexFieldData.getFieldName() + "]");
                        } else {
                            int docCount = docValues.docValueCount();
                            for (int j = 0; j < docCount; j++) {
                                fieldValues.add(docValues.nextValue());
                            }

                            float rescore = TagMatctUtil.score(context.kvPairs, fieldValues,
                                null, context.kvOp, context.mergeOp);
                            topDocs.scoreDocs[i].score = context.rescoreMode.combine(topDocs
                                    .scoreDocs[i].score * context.queryWeight,
                                rescore * context.rescoreQueryWeight);
                        }
                    }
                }
            }

            // Sort by score descending, then docID ascending, just like lucene's QueryRescorer
            Arrays.sort(topDocs.scoreDocs, (a, b) -> {
                if (a.score > b.score) {
                    return -1;
                }

                if (a.score < b.score) {
                    return 1;
                }

                // Safe because doc ids >= 0
                return a.doc - b.doc;
            });

            return topDocs;
        }

        @Override
        public Explanation explain(int topLevelDocId, IndexSearcher searcher,
            RescoreContext rescoreContext,
            Explanation sourceExplanation) throws IOException {
            TagMatchRescoreContext context = (TagMatchRescoreContext) rescoreContext;
            // Note that this is inaccurate because it ignores factor field
            //TODO: 待完成

            Iterator<LeafReaderContext> leaves = searcher.getIndexReader().leaves().iterator();
            LeafReaderContext leaf = null;
            AtomicFieldData fd = null;
            SortedNumericDocValues docValues = null;
            StringBuilder explainStr = new StringBuilder("tag-match[");
            int endDoc = 0;
            float score = sourceExplanation.getValue();
            if (topLevelDocId >= endDoc) {
                do {
                    leaf = leaves.next();
                    endDoc = leaf.docBase + leaf.reader().maxDoc();
                } while (topLevelDocId >= endDoc);

                fd = context.indexFieldData.load(leaf);
                if (false == (fd instanceof AtomicNumericFieldData)) {
                    throw new IllegalArgumentException(
                        "[" + context.indexFieldData.getFieldName() +
                            "] is not a number");
                }
                docValues = ((AtomicNumericFieldData) fd).getLongValues();
                if (docValues.advanceExact(topLevelDocId)) {
                    List<Object> fieldValues = new ArrayList<>();
                    //TODO: 多个值的数组类型不知道为什么不行，后续在研究
                    if (docValues.docValueCount() > 1) {
                        String message = "document [" + topLevelDocId
                            + "] has more than one value for [" +
                            context.indexFieldData.getFieldName() + "]";
                        explainStr.append(message);
                        throw new IllegalArgumentException(message);
                    } else {
                        int docCount = docValues.docValueCount();
                        for (int j = 0; j < docCount; j++) {
                            fieldValues.add(docValues.nextValue());
                        }


                        float rescore = TagMatctUtil.score(context.kvPairs, fieldValues,
                            null, context.kvOp, context.mergeOp);
                        explainStr.append("rescoreMode: ").append(context.rescoreMode)
                            .append(",queryWeight: ").append(context.queryWeight)
                            .append(",rescoreQueryWeiht: ").append(context.rescoreQueryWeight)
                            .append(",firstScore: ").append(score).append(", secondScore: ")
                            .append(rescore).append("]");
                        score = context.rescoreMode.combine(score * context.queryWeight,
                            rescore * context.rescoreQueryWeight);
                    }
                }
            }


            return Explanation.match(score, explainStr.toString(),
                singletonList(sourceExplanation));
        }

        @Override
        public void extractTerms(IndexSearcher searcher, RescoreContext rescoreContext,
            Set<Term> termsSet) {
            // Since we don't use queries there are no terms to extract.
        }
    }
}
