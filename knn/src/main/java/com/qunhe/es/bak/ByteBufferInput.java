/*
 * ByteBufferInput.java
 * Copyright 2021 Qunhe Tech, all rights reserved.
 * Qunhe PROPRIETARY/CONFIDENTIAL, any form of usage is subject to approval.
 */

package com.qunhe.es.bak;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Function: ${Description}
 *
 * @author wuxiang
 * @date 2021/2/8
 */
public abstract class ByteBufferInput implements RandomAccessInput {
    protected long length;
    protected ByteBuffer[] buffers;
    protected int curBufIndex = -1;
    protected ByteBuffer curBuf;

    public abstract void seek(long pos) throws IOException;

    /** The number of bytes in the file. */
    public abstract long length();

    protected ByteBufferInput(final long length, final ByteBuffer[] buffers, final int curBufIndex,
            final ByteBuffer curBuf) {
        this.length = length;
        this.buffers = buffers;
        this.curBufIndex = curBufIndex;
        this.curBuf = curBuf;
    }

    public static ByteBufferInput newInstance(final String resourceDescription,
            final ByteBuffer[] buffers, final long length, final int chunkSizePower) {
        //TODO
        return null;
    }
}
