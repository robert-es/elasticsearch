/*
 * Directory.java
 * Copyright 2021 Qunhe Tech, all rights reserved.
 * Qunhe PROPRIETARY/CONFIDENTIAL, any form of usage is subject to approval.
 */

package com.qunhe.es.bak;

import com.qunhe.es.store.exceptions.AlreadyClosedException;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Collection;

/**
 * Function: ${Description}
 *
 * @author wuxiang
 * @date 2021/2/8
 */
public abstract class Directory implements Closeable {
    @Override
    public void close() throws IOException {

    }

    /**
     * 列出当前目录下的所有文件
     * @return
     * @throws IOException
     */
    public abstract String[] listAll() throws IOException;

    /**
     * 删除目前下已经存在的文件
     * @param name
     * @throws IOException
     */
    public abstract void deleteFile(String name) throws IOException;

//    public abstract IndexOutput createOutput(String name, IOContext context) throws IOException;

    /**
     * 将数据的变更同步到磁盘
     * @param names
     * @throws IOException
     */
    public abstract void sync(Collection<String> names) throws IOException;

    /**
     * 获取当前目录的锁
     * @param name
     * @return
     * @throws IOException
     */
    public abstract Lock obtainLock(String name) throws IOException;

    /**
     * 确保目录是打开的
     * @throws AlreadyClosedException
     */
    protected abstract void ensureOpen() throws AlreadyClosedException;

    public abstract Path getDirectory();

    public abstract ByteBufferInput openInput(String name, IOContext context) throws IOException;

    public abstract ByteBufferInput createOutput(String name, IOContext context) throws IOException;

    /**
     * Returns the byte length of a file in the directory.
     *
     * This method must throw either {@link NoSuchFileException} or {@link FileNotFoundException}
     * if {@code name} points to a non-existing file.
     *
     * @param name the name of an existing file.
     * @throws IOException in case of I/O error
     */
    public abstract long fileLength(String name) throws IOException;


}
