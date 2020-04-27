/*
 * TagMatchScriptFactory.java
 * Copyright 2020 Qunhe Tech, all rights reserved.
 * Qunhe PROPRIETARY/CONFIDENTIAL, any form of usage is subject to approval.
 */

package com.qunhe.es.plugins.script;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Map;

import static com.qunhe.es.plugins.constant.Const.DOC_FIELD_TYPE;
import static com.qunhe.es.plugins.script.TagMatchScript.DOC_FIELD;
import static com.qunhe.es.plugins.script.TagMatchScript.KEYS;
import static com.qunhe.es.plugins.script.TagMatchScript.KV_OP;
import static com.qunhe.es.plugins.script.TagMatchScript.MERGE_OP;
import static com.qunhe.es.plugins.script.TagMatchScript.VALUES;

/**
 * Function: ${Description}
 *
 * @author wuxiang
 * @date 2020/4/17
 */
public class TagMatchScriptFactory implements ScoreScript.LeafFactory {
    private final Map<String, Object> params;
    private final SearchLookup lookup;

    public TagMatchScriptFactory(Map<String, Object> params, SearchLookup lookup) {
        this.params = params;
        this.lookup = lookup;

        if (!params.containsKey(DOC_FIELD)) {
            throw new IllegalArgumentException("Missing parameter [" + DOC_FIELD + "]");
        }

        if (!params.containsKey(DOC_FIELD_TYPE)) {
            throw new IllegalArgumentException("Missing parameter [" + DOC_FIELD_TYPE + "]");
        }

        if (!params.containsKey(KEYS)) {
            throw new IllegalArgumentException("Missing parameter [" + KEYS + "]");
        }

        if (!params.containsKey(VALUES)) {
            throw new IllegalArgumentException("Missing parameter [" + VALUES + "]");
        }

        if (!params.containsKey(KV_OP)) {
            throw new IllegalArgumentException("Missing parameter [" + KV_OP + "]");
        }

        if (!params.containsKey(MERGE_OP)) {
            throw new IllegalArgumentException("Missing parameter [" + MERGE_OP + "]");
        }
    }


    @Override
    public ScoreScript newInstance(final LeafReaderContext leafReaderContext) throws IOException {
        return new TagMatchScript(params, lookup, leafReaderContext);
    }

    @Override
    public boolean needs_score() {
        return false;
    }
}
