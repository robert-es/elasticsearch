/*
 * TagMatchPlugin.java
 * Copyright 2020 Qunhe Tech, all rights reserved.
 * Qunhe PROPRIETARY/CONFIDENTIAL, any form of usage is subject to approval.
 */

package com.qunhe.es.plugins;

import com.qunhe.es.plugins.query.TagMatchQueryBuilder;
import com.qunhe.es.plugins.script.TagMatchEngine;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;


/**
 * Function: ${Description}
 *
 * @author wuxiang
 * @date 2020/4/17
 */
public class TagMatchPlugin extends Plugin implements SearchPlugin, ScriptPlugin {
    @Override
    public List<QuerySpec<?>> getQueries() {
        return Arrays.asList(
                new QuerySpec<>(TagMatchQueryBuilder.NAME, TagMatchQueryBuilder::new,
                        TagMatchQueryBuilder::fromXContent));
    }

    @Override
    public ScriptEngine getScriptEngine(final Settings settings,
            final Collection<ScriptContext<?>> contexts) {
        return new TagMatchEngine();
    }

    public static void main(String[] args) {
        System.out.println("Sdkl");
    }
}
