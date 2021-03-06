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

package org.elasticsearch.painless.node;

import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.Scope;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.ListInitializationNode;
import org.elasticsearch.painless.lookup.PainlessConstructor;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.lookup.def;
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.painless.lookup.PainlessLookupUtility.typeToCanonicalTypeName;

/**
 * Represents a list initialization shortcut.
 */
public class EListInit extends AExpression {

    protected final List<AExpression> values;

    public EListInit(Location location, List<AExpression> values) {
        super(location);

        this.values = Collections.unmodifiableList(Objects.requireNonNull(values));
    }

    @Override
    Output analyze(ClassNode classNode, ScriptRoot scriptRoot, Scope scope, Input input) {
        Output output = new Output();

        if (input.read == false) {
            throw createError(new IllegalArgumentException("Must read from list initializer."));
        }

        output.actual = ArrayList.class;

        PainlessConstructor constructor = scriptRoot.getPainlessLookup().lookupPainlessConstructor(output.actual, 0);

        if (constructor == null) {
            throw createError(new IllegalArgumentException(
                    "constructor [" + typeToCanonicalTypeName(output.actual) + ", <init>/0] not found"));
        }

        PainlessMethod method = scriptRoot.getPainlessLookup().lookupPainlessMethod(output.actual, false, "add", 1);

        if (method == null) {
            throw createError(new IllegalArgumentException("method [" + typeToCanonicalTypeName(output.actual) + ", add/1] not found"));
        }

        List<Output> valueOutputs = new ArrayList<>(values.size());

        for (AExpression expression : values) {
            Input expressionInput = new Input();
            expressionInput.expected = def.class;
            expressionInput.internal = true;
            Output expressionOutput = expression.analyze(classNode, scriptRoot, scope, expressionInput);
            expression.cast(expressionInput, expressionOutput);
            valueOutputs.add(expressionOutput);
        }

        ListInitializationNode listInitializationNode = new ListInitializationNode();

        for (int i = 0; i < values.size(); ++i) {
            listInitializationNode.addArgumentNode(values.get(i).cast(valueOutputs.get(i)));
        }

        listInitializationNode.setLocation(location);
        listInitializationNode.setExpressionType(output.actual);
        listInitializationNode.setConstructor(constructor);
        listInitializationNode.setMethod(method);

        output.expressionNode = listInitializationNode;

        return output;
    }
}
