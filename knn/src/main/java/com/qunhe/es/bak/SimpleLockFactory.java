/*
 * SimpleLockFactory.java
 * Copyright 2021 Qunhe Tech, all rights reserved.
 * Qunhe PROPRIETARY/CONFIDENTIAL, any form of usage is subject to approval.
 */

package com.qunhe.es.bak;

import com.qunhe.es.store.exceptions.AlreadyClosedException;
import com.qunhe.es.store.exceptions.LockObtainFailedException;
import com.qunhe.es.store.exceptions.LockReleaseFailedException;

import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;

/**
 * Function: ${Description}
 *
 * @author wuxiang
 * @date 2021/2/8
 */
public class SimpleLockFactory extends LockFactory {
    /**
     * Singleton instance
     */
    private static final SimpleLockFactory INSTANCE = new SimpleLockFactory();

    private SimpleLockFactory() {
    }

    public static SimpleLockFactory getInstance() {
        return INSTANCE;
    }

    @Override
    public Lock obtainLock(final Directory dir, final String lockName) throws IOException {
        Path lockDir = dir.getDirectory();

        // Ensure that lockDir exists and is a directory.
        // note: this will fail if lockDir is a symlink
        Files.createDirectories(lockDir);

        Path lockFile = lockDir.resolve(lockName);

        // create the file: this will fail if it already exists
        try {
            Files.createFile(lockFile);
        } catch (FileAlreadyExistsException | AccessDeniedException e) {
            // convert optional specific exception to our optional specific exception
            throw new LockObtainFailedException("Lock held elsewhere: " + lockFile, e);
        }

        // used as a best-effort check, to see if the underlying file has changed
        final FileTime creationTime = Files.readAttributes(lockFile, BasicFileAttributes.class)
                .creationTime();

        return new SimpleLockFactory.SimpleFSLock(lockFile, creationTime);
    }

    static final class SimpleFSLock extends Lock {
        private final Path path;
        private final FileTime creationTime;
        private volatile boolean closed;

        SimpleFSLock(Path path, FileTime creationTime) throws IOException {
            this.path = path;
            this.creationTime = creationTime;
        }

        @Override
        public void ensureValid() throws IOException {
            if (closed) {
                throw new AlreadyClosedException("Lock instance already released: " + this);
            }
            // try to validate the backing file name, that it still exists,
            // and has the same creation time as when we obtained the lock.
            // if it differs, someone deleted our lock file (and we are ineffective)
            FileTime ctime = Files.readAttributes(path, BasicFileAttributes.class).creationTime();
            if (!creationTime.equals(ctime)) {
                throw new AlreadyClosedException(
                        "Underlying file changed by an external force at " + ctime + ", (lock="
                                + this + ")");
            }
        }

        @Override
        public synchronized void close() throws IOException {
            if (closed) {
                return;
            }
            try {
                // NOTE: unlike NativeFSLockFactory, we can potentially delete someone else's
                // lock if things have gone wrong. we do best-effort check (ensureValid) to
                // avoid doing this.
                try {
                    ensureValid();
                } catch (Throwable exc) {
                    // notify the user they may need to intervene.
                    throw new LockReleaseFailedException(
                            "Lock file cannot be safely removed. Manual intervention is recommended.",
                            exc);
                }
                // we did a best effort check, now try to remove the file. if something goes wrong,
                // we need to make it clear to the user that the directory may still remain locked.
                try {
                    Files.delete(path);
                } catch (Throwable exc) {
                    throw new LockReleaseFailedException(
                            "Unable to remove lock file. Manual intervention is recommended", exc);
                }
            } finally {
                closed = true;
            }
        }
    }
}
