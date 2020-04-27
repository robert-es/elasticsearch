/*
 * TagMatchEngine.java
 * Copyright 2020 Qunhe Tech, all rights reserved.
 * Qunhe PROPRIETARY/CONFIDENTIAL, any form of usage is subject to approval.
 */

package com.qunhe.es.plugins.script;

import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;

import java.util.Map;

/**
 * Function: ${Description}
 *
 * @author wuxiang
 * @date 2020/4/17
 */
public class TagMatchEngine implements ScriptEngine {
    private final static String LANG_TYPE = "tag-match";
    private final static String SCRIPT_SOURCE = "tag-match";

    @Override
    public String getType() {
        return LANG_TYPE;
    }

    @Override
    public <FactoryType> FactoryType compile(final String scriptName, final String scriptSource,
            final ScriptContext<FactoryType> scriptContext, final Map<String, String> map) {
        if (!scriptContext.equals(ScoreScript.CONTEXT)) {
            throw new IllegalArgumentException(
                    getType() + " scripts cannot be used for context [" + scriptContext.name + "]");
        }

        // we use the script "source" as the script identifier
        if (SCRIPT_SOURCE.equals(scriptSource)) {
            ScoreScript.Factory factory = TagMatchScriptFactory::new;
            return scriptContext.factoryClazz.cast(factory);
        }

        throw new IllegalArgumentException("Unknown script name " + scriptSource);
    }
}
