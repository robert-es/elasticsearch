/*
 * TagMatchQuery.java
 * Copyright 2020 Qunhe Tech, all rights reserved.
 * Qunhe PROPRIETARY/CONFIDENTIAL, any form of usage is subject to approval.
 */

package com.qunhe.es.plugins.query;

import com.qunhe.es.plugins.constant.KvOp;
import com.qunhe.es.plugins.constant.MergeOp;
import com.qunhe.es.plugins.util.CollectionUtils;
import com.qunhe.es.plugins.util.TagMatctUtil;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.logging.Loggers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.qunhe.es.plugins.constant.Const.DEFAULT_SCORE;
import static com.qunhe.es.plugins.util.TagMatctUtil.parseKvPairs;

/**
 * Function: ${Description}
 *
 * @author wuxiang
 * @date 2020/4/22
 */
public class TagMatchQuery extends Query {
    private static final Logger LOG = Loggers.getLogger(TagMatchQuery.class, "[tag-match]");

    public final static String LIST_STR_TYPE = "string_list";
    public final static String SPLIT_SPACE = ",";

    private List<Long> keys;
    private List<Float> values;
    private String field;
    private String scoreField;
    private String fieldType;
    private KvOp kvOp;
    private MergeOp mergeOp;
    private Map<String, Float> kvPairs;

    @Override
    public String toString(final String s) {
        return "TagMatchQuery{" + s + "}";
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (!sameClassAs(obj)) {
            return false;
        }

        TagMatchQuery query = (TagMatchQuery) obj;

        return field.equals(query.field) && fieldType.equals(query.fieldType) && kvOp.equals(
            query.kvOp) && mergeOp.equals(query.mergeOp) && Objects.deepEquals(keys, query
            .keys) && Objects.deepEquals(values, query.keys);
    }

    @Override
    public int hashCode() {
        return 31 * classHash() + Objects.hash(field, fieldType, kvOp, mergeOp, keys, values);
    }

    static TagMatchQuery build(final String field, final String scoreField, final String
        fieldType, final KvOp kvOp, MergeOp mergeO, final List<Long> keys,
        final List<Float> values) {
        TagMatchQuery query = new TagMatchQuery();

        query.field = field;
        query.scoreField = scoreField;
        query.fieldType = fieldType;
        query.kvOp = kvOp;
        query.mergeOp = mergeO;
        query.keys = keys;
        query.values = values;

        if (CollectionUtils.isNotEmpty(keys)) {
            query.kvPairs = parseKvPairs(query.kvOp, keys, values);
        }

        return query;
    }


    @Override
    public Weight createWeight(final IndexSearcher searcher, final boolean needsScores,
        final float boost) throws IOException {
        if (!needsScores) {
            // If scores are not needed simply return a constant score on all docs
            return new ConstantScoreWeight(this, boost) {
                @Override
                public Scorer scorer(LeafReaderContext context) throws IOException {
                    return new ConstantScoreScorer(this, score(), DocIdSetIterator
                        .all(context.reader().maxDoc()));
                }

                @Override
                public boolean isCacheable(LeafReaderContext ctx) {
                    return true;
                }
            };
        } else {
            RankerWeight rankerWeight = new RankerWeight(field, kvOp, mergeOp, kvPairs);
            return rankerWeight;
        }
    }

    public class RankerWeight extends Weight {
        private final String field;
        private final KvOp kvOp;
        private final MergeOp mergeOp;
        private final Map<String, Float> kvPairs;

        public RankerWeight(final String field, final KvOp kvOp, final MergeOp mergeOp,
            Map<String, Float> kvPairs) {
            super(TagMatchQuery.this);
            this.field = field;
            this.kvOp = kvOp;
            this.mergeOp = mergeOp;
            this.kvPairs = kvPairs;
        }

        @Override
        public void extractTerms(final Set<Term> set) {

        }

        @Override
        public Explanation explain(final LeafReaderContext leafReaderContext, final int docId)
            throws IOException {

            SortedNumericDocValues docValues = DocValues.getSortedNumeric(
                leafReaderContext.reader(), field);
            List<Object> fieldValues = new ArrayList<>();
            StringBuilder explainStr = new StringBuilder("tag-match[");
            if (docValues.advanceExact(docId)) {
                fieldValues.add(docValues.nextValue());
                explainStr.append("fieldValue: ").append(fieldValues.get(0));
            } else {
                explainStr.append("fieldValue: not found");
            }
            float score = TagMatctUtil.score(kvPairs, fieldValues, null, kvOp, mergeOp);
            explainStr.append(", score: ").append(score).append("]");

            return Explanation.match(score, explainStr.toString());
        }


        /**
         * 这个LeafReaderContext是索引前缀树的叶子节点指向的term dictionary 的block，
         * rescore处理过程中，在调用score方法之前，通过iterator()
         * 方法获取该block对应的DocIdSetIterator，然后调用DocIdSetIterator.advance(要打分文档的docId)，
         * 我们在业务代码中，通过docId()即可获取要打分文档的id
         * @return
         * @throws IOException
         */
        @Override
        public Scorer scorer(final LeafReaderContext context) throws IOException {
            DocIdSetIterator approximation = DocIdSetIterator.all(context.reader().maxDoc());
            return new RankerScorer(context, approximation, field, kvOp, mergeOp, kvPairs);
        }

        @Override
        public boolean isCacheable(final LeafReaderContext leafReaderContext) {
            return false;
        }

        class RankerScorer extends Scorer {
            private final LeafReaderContext context;
            private final DocIdSetIterator docIdSetIterator;
            private final String field;
            private final KvOp kvOp;
            private final MergeOp mergeOp;
            private final Map<String, Float> kvPairs;

            public RankerScorer(final LeafReaderContext context,
                final DocIdSetIterator docIdSetIterator, final String field, final KvOp kvOp,
                final MergeOp mergeOp, Map<String, Float> kvPairs) {
                super(RankerWeight.this);
                this.context = context;
                this.docIdSetIterator = docIdSetIterator;
                this.field = field;
                this.kvOp = kvOp;
                this.mergeOp = mergeOp;
                this.kvPairs = kvPairs;
            }

            @Override
            public int docID() {
                return docIdSetIterator.docID();
            }

            @Override
            public float score() throws IOException {
                List<Object> fieldValues = new ArrayList<>();
                List<Float> fieldScores = null;
                int docId = docID();
                final FieldInfo fieldInfo = context.reader().getFieldInfos().fieldInfo(field);
                if (null == fieldInfo) {
                    return 0;
                }
                final DocValuesType docValuesType = fieldInfo.getDocValuesType();
                if (DocValuesType.SORTED_NUMERIC.equals(docValuesType)) {
                    //数值的list类型
                    SortedNumericDocValues docValues = DocValues.getSortedNumeric(context.reader(),
                        field);
                    if (docValues.advanceExact(docId)) {
                        int count = docValues.docValueCount();
                        for (int i = 0; i < count; i++) {
                            fieldValues.add(docValues.nextValue());
                        }
                    }
                } else if (DocValuesType.NUMERIC.equals(docValuesType)) {
                    //单字数值类型
                    SortedNumericDocValues docValues = DocValues.getSortedNumeric(context.reader(),
                        field);
                    if (docValues.advanceExact(docId)) {
                        fieldValues.add(docValues.nextValue());
                    }
                } else if (DocValuesType.SORTED_SET.equals(docValuesType)) {
                    //字符串类型
                    StringBuilder fieldValueBuilder = new StringBuilder();
                    SortedSetDocValues docValues = DocValues.getSortedSet(context.reader(), field);
                    if (docValues.advanceExact(docId)) {
                        long ord;
                        while ((ord = docValues.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
                            BytesRef bytesRef = docValues.lookupOrd(ord);
                            fieldValueBuilder.append(bytesRef.utf8ToString());
                        }
                    }
                    String fieldValue = fieldValueBuilder.toString();
                    if (LIST_STR_TYPE.equals(fieldType)) {
                        String[] valueAndScores = fieldValue.split(SPLIT_SPACE);
                        if (!(valueAndScores.length % 2 == 0)) {
                            LOG.error(field + "字段值错误，字段切割后，数量不是偶数!");
                            return 0;
                        }

                        fieldScores = new ArrayList<>();
                        for (int i = 0; i < valueAndScores.length; i += 2) {
                            fieldValues.add(valueAndScores[i].trim());
                            Float fieldScore = Float.parseFloat(valueAndScores[i + 1].trim());
                            fieldScores.add(fieldScore);
                        }
                    }
                }

                float score = TagMatctUtil.score(kvPairs, fieldValues, fieldScores, kvOp, mergeOp);


                return score;
            }

            @Override
            public DocIdSetIterator iterator() {
                return docIdSetIterator;
            }
        }
    }
}
