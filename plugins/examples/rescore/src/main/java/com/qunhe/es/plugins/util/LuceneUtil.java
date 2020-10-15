/*
 * LuceneUtil.java
 * Copyright 2020 Qunhe Tech, all rights reserved.
 * Qunhe PROPRIETARY/CONFIDENTIAL, any form of usage is subject to approval.
 */

package com.qunhe.es.plugins.util;

import org.apache.lucene.analysis.payloads.PayloadHelper;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.List;

/**
 * Function: ${Description}
 *
 * @author wuxiang
 * @date 2020/4/26
 */
public class LuceneUtil {
    public static void getPayloadFromIndex(final LeafReader reader, final String field,
            final List<Object> fieldValues, final List<Float> fieldScores) throws IOException {
        Terms terms = reader.terms(field);
        TermsEnum iterator = terms.iterator();
        BytesRef idTerm;
        PostingsEnum postingsEnum = null;
        while ((idTerm = iterator.next()) != null) {
            postingsEnum = iterator.postings(postingsEnum, PostingsEnum.PAYLOADS);
            postingsEnum.nextPosition();
            BytesRef payload = postingsEnum.getPayload();
            if (null != payload) {
                fieldValues.add(idTerm.utf8ToString());
                fieldScores.add(PayloadHelper.decodeFloat(payload.bytes, payload.offset));
            }
        }
    }
}
