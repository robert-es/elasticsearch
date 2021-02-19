/*
 * CodingRestHandler.java
 * Copyright 2021 Qunhe Tech, all rights reserved.
 * Qunhe PROPRIETARY/CONFIDENTIAL, any form of usage is subject to approval.
 */

package com.qunhe.es.plugins.knn.pq;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.SpecialPermission;


import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;

/**
 * Function: ${Description}
 *
 * @author wuxiang
 * @date 2021/2/5
 */
public class PqRestHandlerPlugin extends Plugin implements ActionPlugin {
    private static final Logger LOG = Loggers.getLogger(PqRestHandlerPlugin.class, "[knn-pq]");
    private static boolean pqUseable = false;
    private Path pqPath;
    private final String KNN_PATH_NAME = "qunhe_knn";


    @Override
    public List<RestHandler> getRestHandlers(final Settings settings,
            final RestController restController,
            final ClusterSettings clusterSettings, final IndexScopedSettings indexScopedSettings,
            final SettingsFilter settingsFilter,
            final IndexNameExpressionResolver indexNameExpressionResolver,
            final Supplier<DiscoveryNodes> nodesInCluster) {
        return singletonList(new PqAction());
    }

    @Override
    public Collection<Object> createComponents(final Client client,
            final ClusterService clusterService,
            final ThreadPool threadPool, final ResourceWatcherService resourceWatcherService,
            final ScriptService scriptService, final NamedXContentRegistry xContentRegistry,
            final Environment environment, final NodeEnvironment nodeEnvironment,
            final NamedWriteableRegistry namedWriteableRegistry,
            final IndexNameExpressionResolver indexNameExpressionResolver,
            final Supplier<RepositoriesService> repositoriesServiceSupplier) {

        Path[] paths = environment.dataFiles();
        if (null == paths || paths.length == 0) {
            LOG.error("knn-pq plugin 获取 data.path路径失败, knn-pq插件不可用");
        } else {
            final Path dataPath = paths[0];

            SecurityManager sm = System.getSecurityManager();
            if (sm != null) {
                // unprivileged code such as scripts do not have SpecialPermission
                sm.checkPermission(new SpecialPermission());
            }
            /**
             * 参考：https://www.elastic.co/guide/en/elasticsearch/plugins/2.2/plugin-authors.html#_java_security_permissions
             */
            AccessController.doPrivileged(new PrivilegedAction<Void>() {
                @Override
                public Void run() {
                    try {
                        /**
                         * 与data path创建同级目录
                         */
                        Path knnPath = Files.createDirectories(
                                dataPath.resolveSibling(KNN_PATH_NAME));
                        pqPath = Files.createFile(Paths.get(knnPath.toString(), "knn.pq"));
                        Files.write(pqPath, "sdlkksdk".getBytes(), StandardOpenOption.APPEND);
                    } catch (Exception e) {
                        LOG.error(String.format("创建%s路径失败", pqPath));
                        pqUseable = false;
                    }finally {

                    }
                    return null;
                }
            });
        }

        return Collections.emptyList();
    }
}
