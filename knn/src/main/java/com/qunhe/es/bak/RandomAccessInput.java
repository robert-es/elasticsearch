/*
 * RandomAccessInput.java
 * Copyright 2021 Qunhe Tech, all rights reserved.
 * Qunhe PROPRIETARY/CONFIDENTIAL, any form of usage is subject to approval.
 */

package com.qunhe.es.bak;

import org.apache.lucene.store.DataInput;

import java.io.IOException;

/**
 * Function: ${Description}
 *
 * @author wuxiang
 * @date 2021/2/8
 */
public interface RandomAccessInput {
    /**
     * Reads a byte at the given position in the file
     * @see DataInput#readByte
     */
    public byte readByte(long pos) throws IOException;
    /**
     * Reads a short at the given position in the file
     * @see DataInput#readShort
     */
    public short readShort(long pos) throws IOException;
    /**
     * Reads an integer at the given position in the file
     * @see DataInput#readInt
     */
    public int readInt(long pos) throws IOException;
    /**
     * Reads a long at the given position in the file
     * @see DataInput#readLong
     */
    public long readLong(long pos) throws IOException;
}
