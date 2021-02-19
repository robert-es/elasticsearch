/*
 * KnnQpCoding.java
 * Copyright 2021 Qunhe Tech, all rights reserved.
 * Qunhe PROPRIETARY/CONFIDENTIAL, any form of usage is subject to approval.
 */

package com.qunhe.es.store;

import com.qunhe.es.plugins.knn.pq.PqRestHandlerPlugin;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.common.logging.Loggers;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Function: 向量量化编码
 * http://confluence.qunhequnhe.com/pages/viewpage.action?pageId=80233966866
 * https://yongyuan.name/blog/ann-search.html
 *
 * @author wuxiang
 * @date 2021/2/19
 */
public class KnnQpCoding {
    private static final Logger LOG = Loggers.getLogger(PqRestHandlerPlugin.class, "[knn-pq]");
    private static final String NAME_TEMPLATE = "knn-qp.%s";

    private Path knnQpPath;
    private MMapDirectory directory;
    private IndexOutput out;

    /**
     * 向量维度
     */
    private final int dimNum;
    /**
     * 聚类数量
     */
    private final int classifyNum;
    /**
     * 子空间维度
     */
    private final int subDimNum;

    /**
     * 子空间数量 = dimNum/subDimNum
     */
    private final int subSegmentCount;
    /**
     * 编码数据版本
     */
    private final String version;

    /**
     * 子空间聚类的数量
     */
    private final int subClassifyNum;

    private final boolean preLoad = true;

    /**
     * 每个子空间距离表数据量
     */
    private final long distanceTableCount;

    private AtomicBoolean searchAble = new AtomicBoolean(false);
    private int currentWriteIndex = 0;

    public KnnQpCoding(final int dimNum, final int classifyNum, final int subDimNum,
            final int subClassifyNum, final String knnQpPath, final String version) {
        assert subDimNum > 0;
        assert dimNum % subDimNum == 0;

        this.dimNum = dimNum;
        this.classifyNum = classifyNum;
        this.subDimNum = subDimNum;
        this.subSegmentCount = dimNum / subDimNum;
        this.subClassifyNum = subClassifyNum;
        this.version = version;
        /**
         * 注意不是 n * n， 由于是两两之间的距离，A到B 与B到A的距离是一样的，不用重复存储
         */
        this.distanceTableCount = this.subClassifyNum * (this.subClassifyNum - 1) / 2;

        try {
            this.knnQpPath = Paths.get(knnQpPath);
            this.directory = new MMapDirectory(this.knnQpPath);
            final String name = String.format(NAME_TEMPLATE, version);
            this.out = directory.createOutput(name, IOContext.DEFAULT);
        } catch (Exception e) {
            LOG.error("向量量化编码数据加载缓存失败", e);
            searchAble.set(false);
        }
    }

    /**
     * TODO: 加锁
     * 保存第1个子空间中心与第2个子空间中心的距离, 好像可以缩减一半的数据量，在想想
     * @param classifyId 聚类id
     * @param subSegmentId  子空间分段id
     * @param firstSubClassifyId 第1个子空间id
     * @param secondSubClassifyId 第2个子空间id
     * @return
     */
    public boolean writeQpValue(final int classifyId, int subSegmentId, final int
            firstSubClassifyId, final int secondSubClassifyId, final double distance) {
        synchronized (this) {
            long writeIndex = classifyId * subSegmentId;
            final int minSubClassifyId = Math.min(firstSubClassifyId, secondSubClassifyId);
            final int maxSubClassifyId = Math.max(firstSubClassifyId, secondSubClassifyId);
            writeIndex = writeIndex + subClassifyNum * (subClassifyNum - minSubClassifyId);
//            if (this.currentWriteIndex ==)
        }

        return false;
    }

    /**
     * 获取两个subClassId写入缓存的位置
     * @param classifyId 从0开始
     * @param subSegmentId 从0开始
     * @param firstSubClassifyId
     * @param secondSubClassifyId
     * @return
     */
    public long getBufferPosition(final int classifyId, int subSegmentId, final int
            firstSubClassifyId, final int secondSubClassifyId) {
        if (firstSubClassifyId == secondSubClassifyId) {
            /**
             * 同样一个空间不需要存储距离
             */
            return -1L;
        }

        final int minSubClassifyId = Math.min(firstSubClassifyId, secondSubClassifyId);
        final int maxSubClassifyId = Math.max(firstSubClassifyId, secondSubClassifyId);
        long writePosition = (classifyId + 1) * subSegmentId * this.distanceTableCount;
        /**
         * 单个子空间距离表位置
         */
        long distanceTablePosition =
                minSubClassifyId * (2 * this.subClassifyNum - minSubClassifyId) / 2;
//        writePosition = writePosition +

        return -1L;
    }

//    public double getQpValue() {
//
//    }
}
