/*
 * KnnQpDistanceTleab.java
 * Copyright 2021 Qunhe Tech, all rights reserved.
 * Qunhe PROPRIETARY/CONFIDENTIAL, any form of usage is subject to approval.
 */

package com.qunhe.es.store;

import com.qunhe.es.plugins.knn.pq.PqRestHandlerPlugin;
import com.qunhe.es.util.ByteArrayConveter;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.common.logging.Loggers;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Function: 向量量化编码
 * http://confluence.qunhequnhe.com/pages/viewpage.action?pageId=80233966866
 * https://yongyuan.name/blog/ann-search.html
 *
 * @author wuxiang
 * @date 2021/2/19
 */
public class KnnQpDistanceTable {
    private static final Logger LOG = Loggers.getLogger(PqRestHandlerPlugin.class, "[knn-pq]");
    private static final String NAME_TEMPLATE = "knn-qp.%s";

    private Path knnQpPath;
    private MMapDirectory directory;
    private IndexOutput writer;

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

    /**
     * 2倍的subClassifyNum， 提前算好，优化性能用的
     */
    private final long subClassifyNum_X_2;

    private final boolean preLoad = true;

    /**
     * 每个子空间距离表数据量
     */
    private final int distanceTableCount;

    private AtomicBoolean searchAble = new AtomicBoolean(false);
    /**
     * 全局缓存文件的写位置
     */
    private int nextWritePosition = 0;

    /**
     * 单个数据大小，这里是double类型，是8个字节
     */
    public final static int DATA_SIZE = 8;

    private final static long DEFAULT_POSITION = -1;

    private IndexInput reader;

    public KnnQpDistanceTable(final int dimNum, final int classifyNum, final int subDimNum,
                              final int subClassifyNum, final String knnQpPath, final String version) {
        assert subDimNum > 0;
        assert dimNum % subDimNum == 0;

        this.dimNum = dimNum;
        this.classifyNum = classifyNum;
        this.subDimNum = subDimNum;
        this.subSegmentCount = dimNum / subDimNum;
        this.subClassifyNum = subClassifyNum;
        this.subClassifyNum_X_2 = this.subClassifyNum << 1;
        this.version = version;
        /**
         * 注意不是 n * n， 由于是两两之间的距离，A到B 与B到A的距离是一样的，不用重复存储
         */
        this.distanceTableCount = this.subClassifyNum * (this.subClassifyNum - 1) >> 1;

        try {
            this.knnQpPath = Paths.get(knnQpPath);
            /**
             * 一个编码距离值8位
             */
            final int maxChunkSize = distanceTableCount << 3;
            this.directory = new MMapDirectory(this.knnQpPath, FSLockFactory.getDefault(), maxChunkSize);
            final String fileName = String.format(NAME_TEMPLATE, version);
            this.writer = directory.createOutput(fileName, IOContext.DEFAULT);
        } catch (Exception e) {
            LOG.error("向量量化编码数据加载缓存失败", e);
            searchAble.set(false);
        }
    }

    public void openReader() throws IOException {
        if (null != reader) {
            reader.close();
        }

        final String fileName = String.format(NAME_TEMPLATE, version);
        this.reader = this.directory.openInput(fileName, IOContext.DEFAULT);
    }

    /**
     * 保存第1个子空间中心与第2个子空间中心的距离, 好像可以缩减一半的数据量，在想想
     *
     * @param classifyId          聚类id
     * @param subSegmentId        子空间分段id
     * @param firstSubClassifyId  第1个子空间id
     * @param secondSubClassifyId 第2个子空间id
     * @return
     */
    public boolean writeQpValue(final int classifyId, int subSegmentId, final int
            firstSubClassifyId, final int secondSubClassifyId, final double distance) throws IOException {
        synchronized (this) {
            long writePosition = getWriteBufferPosition(classifyId, subSegmentId, firstSubClassifyId, secondSubClassifyId);
            if (this.nextWritePosition != writePosition) {
                LOG.error("请按照顺序写入距离表!");
                return false;
            } else {
                byte[] bytes = ByteArrayConveter.getByteArray(distance);
                this.writer.writeBytes(bytes, DATA_SIZE);
                this.nextWritePosition += DATA_SIZE;
            }
        }

        return true;
    }

    public boolean ready() {
        try {
            this.writer.close();
            this.openReader();
        } catch (Exception e) {
            LOG.error("关闭写操作失败!");
        }
        this.searchAble.compareAndSet(false, true);
        return this.searchAble.get();
    }

    /**
     * 获取两个subClassId写入缓存的位置
     *
     * @param classifyId          从0开始
     * @param subSegmentId        从0开始
     * @param firstSubClassifyId
     * @param secondSubClassifyId
     * @return
     */
    public long getWriteBufferPosition(final int classifyId, int subSegmentId, final int
            firstSubClassifyId, final int secondSubClassifyId) {
        if (firstSubClassifyId == secondSubClassifyId) {
            /**
             * 同样一个空间不需要存储距离
             */
            return DEFAULT_POSITION;
        }

        final long tableIndex = (classifyId * this.subSegmentCount + subSegmentId);
        final long tablePosition = getDistanceTablePosition(firstSubClassifyId, secondSubClassifyId);
        /**
         * 距离表表index * 距离表表的大小 + 对应两个中心距离数据在距离表的位置
         */
        long bufferPosition = tableIndex * this.distanceTableCount + tablePosition;

        /**
         * 2^3 = 8 = DATA_SIZE
         */
        return bufferPosition << 3;
    }

    /**
     * 子空间编码表各中心之间距离数据在距离表的位置
     *
     * @param firstSubClassifyId
     * @param secondSubClassifyId
     * @return
     */
    private long getDistanceTablePosition(final int firstSubClassifyId, final int secondSubClassifyId) {
        final int minSubClassifyId = Math.min(firstSubClassifyId, secondSubClassifyId);
        final int maxSubClassifyId = Math.max(firstSubClassifyId, secondSubClassifyId);
        /**
         * 单个子空间距离表位置, TODO: 如何优化性能
         */
        long distanceTablePosition =
                ((minSubClassifyId * (this.subClassifyNum_X_2 - minSubClassifyId - 1)) >> 1)
                        + (maxSubClassifyId - minSubClassifyId - 1);

        return distanceTablePosition;
    }

    public double getQpValue(final int classifyId, int subSegmentId, final int
            firstSubClassifyId, final int secondSubClassifyId) throws IOException {
        long writePosition = getWriteBufferPosition(classifyId, subSegmentId, firstSubClassifyId, secondSubClassifyId);

        /**
         * 同一个空间，距离近似为0
         */
        if (DEFAULT_POSITION == writePosition) {
            return 0;
        }
        IndexInput cloneReader = this.reader.clone();
        cloneReader.seek(writePosition);
        byte[] bytes = new byte[DATA_SIZE];
        cloneReader.readBytes(bytes, 0, bytes.length);
        return ByteArrayConveter.getDouble(bytes);
    }
}
