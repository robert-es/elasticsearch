/*
 * MMapDirectory.java
 * Copyright 2021 Qunhe Tech, all rights reserved.
 * Qunhe PROPRIETARY/CONFIDENTIAL, any form of usage is subject to approval.
 */

package com.qunhe.es.bak;

/**
 * Function: ${Description}
 *
 * @author wuxiang
 * @date 2021/2/8
 */
public class MMapDirectory {
//        extends BaseDirectory {
//    protected final Path directory;
//    final int chunkSizePower;
//    private boolean preload;
//
//    private final Set<String>
//            pendingDeletes = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
//
//    /**
//     * Set to {@code true} to ask mapped pages to be loaded
//     * into physical memory on init. The behavior is best-effort
//     * and operating system dependent.
//     * @see MappedByteBuffer#load
//     */
//    public void setPreload(boolean preload) {
//        this.preload = preload;
//    }
//
//    /**
//     * Returns {@code true} if mapped pages should be loaded.
//     * @see #setPreload
//     */
//    public boolean getPreload() {
//        return preload;
//    }
//
//
//    public MMapDirectory(final Path path, final LockFactory lockFactory, int maxChunkSize)
//            throws IOException {
//        super(lockFactory);
//
//        if (!Files.isDirectory(path)) {
//            Files.createDirectories(path);  // create directory, if it doesn't exist
//        }
//
//        this.directory = path.toRealPath();
//
//        this.chunkSizePower = 31 - Integer.numberOfLeadingZeros(maxChunkSize);
//        assert this.chunkSizePower >= 0 && this.chunkSizePower <= 30;
//
//    }
//
//    /**
//     * 用于读取文件对应的
//     * @param name
//     * @param context
//     * @return
//     * @throws IOException
//     */
//    @Override
//    public ByteBufferInput openInput(final String name, final IOContext context)
//            throws IOException {
//        ensureOpen();
//        ensureCanRead(name);
//
//        Path path = directory.resolve(name);
//        try (FileChannel c = FileChannel.open(path, StandardOpenOption.READ)) {
//            final String resourceDescription = "MMapIndexInput(path=\"" + path.toString() + "\")";
//            return ByteBufferInput.newInstance(resourceDescription,
//                    map(resourceDescription, c, 0, c.size()),
//                    c.size(), chunkSizePower);
//        }
//    }
//
//    @Override
//    public ByteBufferInput createOutput(final String name, final IOContext context)
//            throws IOException {
//        ensureOpen();
//        maybeDeletePendingFiles();
//        // If this file was pending delete, we are now bringing it back to life:
//        if (pendingDeletes.remove(name)) {
//            privateDeleteFile(name, true); // try again to delete it - this is best effort
//            pendingDeletes.remove(name); // watch out - if the delete fails it put
//        }
//        return new FSIndexOutput(name);
//    }
//
//    protected void ensureCanRead(String name) throws IOException {
//        if (pendingDeletes.contains(name)) {
//            throw new NoSuchFileException(
//                    "file \"" + name + "\" is pending delete and cannot be opened for read");
//        }
//    }
//
//    private void maybeDeletePendingFiles() throws IOException {
//        //TODO
//    }
//
//    public static MMapDirectory open(Path path, LockFactory lockFactory) throws IOException {
//        return new MMapDirectory(path, lockFactory, 2);
//    }
//
//    /**
//     *  将文件映射成 ByteBuffer[]， 防止单个ByteBuffer 过大
//     * */
//    final ByteBuffer[] map(String resourceDescription, FileChannel fc, long offset, long length)
//            throws IOException {
//        /**
//         * length / 2^chunkSizePower
//         */
//        if ((length >>> chunkSizePower) >= Integer.MAX_VALUE) {
//            throw new IllegalArgumentException(
//                    "RandomAccessFile too big for chunk size: " + resourceDescription);
//        }
//
//        final long chunkSize = 1L << chunkSizePower;
//
//        // we always allocate one more buffer, the last one may be a 0 byte one
//        final int nrBuffers = (int) (length >>> chunkSizePower) + 1;
//
//        ByteBuffer buffers[] = new ByteBuffer[nrBuffers];
//
//        long bufferStart = 0L;
//        for (int bufNr = 0; bufNr < nrBuffers; bufNr++) {
//            int bufSize = (int) ((length > (bufferStart + chunkSize))
//                    ? chunkSize : (length - bufferStart));
//            MappedByteBuffer buffer;
//            try {
//                buffer = fc.map(FileChannel.MapMode.READ_ONLY, offset + bufferStart, bufSize);
//            } catch (IOException ioe) {
//                // todo log
//                throw ioe;
//            }
//
//            if (preload) {
//                buffer.load();
//            }
//            buffers[bufNr] = buffer;
//            bufferStart += bufSize;
//        }
//
//        return buffers;
//    }
//
//    @Override
//    public String[] listAll() throws IOException {
//        return new String[0];
//    }
//
//    @Override
//    public void deleteFile(final String name) throws IOException {
//
//    }
//
//    @Override
//    public void sync(final Collection<String> names) throws IOException {
//
//    }
//
//    protected void fsync(String name) throws IOException {
//        IOUtils.fsync(directory.resolve(name), false);
//    }
//
//    @Override
//    public Path getDirectory() {
//        ensureOpen();
//        return directory;
//    }
}
