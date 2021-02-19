/*
 * PqAction.java
 * Copyright 2021 Qunhe Tech, all rights reserved.
 * Qunhe PROPRIETARY/CONFIDENTIAL, any form of usage is subject to approval.
 */

package com.qunhe.es.plugins.knn.pq;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.cat.AbstractCatAction;
import org.elasticsearch.rest.action.cat.RestTable;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * Function: ${Description}
 *
 * @author wuxiang
 * @date 2021/2/7
 */
public class PqAction extends AbstractCatAction {

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(
                new Route(GET, "/_cat/example"),
                new Route(POST, "/_cat/example")));
    }

    @Override
    public String getName() {
        return "rest_handler_cat_example";
    }

    @Override
    protected RestChannelConsumer doCatRequest(final RestRequest request, final NodeClient client) {
        final String message = request.param("message", "Hello from Cat Example action");
        Environment environment = new Environment(null, null);
        environment.dataFiles();
        Table table = getTableWithHeader(request);
        table.startRow();
        table.addCell(message);
        table.endRow();
        return channel -> {
            try {
                channel.sendResponse(RestTable.buildResponse(table, channel));
            } catch (final Exception e) {
                channel.sendResponse(new BytesRestResponse(channel, e));
            }
        };
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append(documentation());
    }

    public static String documentation() {
        return "/_cat/example\n";
    }

    @Override
    protected Table getTableWithHeader(RestRequest request) {
        final Table table = new Table();
        table.startHeaders();
        table.addCell("test", "desc:test");
        table.endHeaders();
        return table;
    }
}
