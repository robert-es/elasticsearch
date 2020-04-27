/*
 * CollectionUtils.java
 * Copyright 2020 Qunhe Tech, all rights reserved.
 * Qunhe PROPRIETARY/CONFIDENTIAL, any form of usage is subject to approval.
 */

package com.qunhe.es.plugins.util;

import java.util.Collection;

/**
 * 插件不想依赖太多的第三方jar包，防止es集群加载太多的依赖
 *
 * @author wuxiang
 * @date 2020/4/20
 */
public class CollectionUtils {
    public static boolean isEmpty(Collection collection) {
        if (null == collection || collection.size() == 0) {
            return true;
        }

        return false;
    }

    public static boolean isNotEmpty(Collection collection) {
        return !isEmpty(collection);
    }
}
