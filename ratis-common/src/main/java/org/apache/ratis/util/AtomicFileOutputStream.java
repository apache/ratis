/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ratis.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A {@link FilterOutputStream} that writes to a file atomically.
 * The output file will not show up until it has been entirely written and sync'ed to disk.
 * It uses a temporary file when it is being written.
 * The default temporary file has a .tmp suffix.
 * <p>
 * When the output stream is closed, it is
 * (1) flushed
 * (2) sync'ed, and
 * (3) renamed/moved from the temporary file to the output file.
 * If the output file already exists, it will be overwritten.
 * <p>
 * NOTE that on Windows platforms, the output file, if it exists, is deleted
 * before the temporary file is moved.
 */
public class AtomicFileOutputStream extends FilterOutputStream {
  static final Logger LOG = LoggerFactory.getLogger(AtomicFileOutputStream.class);

  public static final String TMP_EXTENSION = ".tmp";

  public static File getTemporaryFile(File outFile) {
    return new File(outFile.getParentFile(), outFile.getName() + TMP_EXTENSION);
  }

  private final File outFile;
  private final File tmpFile;
  private final AtomicBoolean isClosed = new AtomicBoolean();

  public AtomicFileOutputStream(File outFile) throws IOException {
    this(outFile, getTemporaryFile(outFile));
  }

  public AtomicFileOutputStream(File outFile, File tmpFile) throws IOException {
    super(FileUtils.newOutputStreamForceAtClose(tmpFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE));
    this.outFile = outFile.getAbsoluteFile();
    this.tmpFile = tmpFile.getAbsoluteFile();
  }

  public boolean isClosed() {
    return isClosed.get();
  }

  @Override
  public void close() throws IOException {
    if (!isClosed.compareAndSet(false, true)) {
      return;
    }
    try {
      super.close();
      FileUtils.move(tmpFile, outFile, StandardCopyOption.REPLACE_EXISTING);
    } catch (Exception e) {
      try {
        FileUtils.deleteIfExists(tmpFile);
      } catch (IOException ioe) {
        e.addSuppressed(ioe);
      }
      throw e;
    }
  }

  /**
   * Close the atomic file, but do not "commit" the temporary file
   * on top of the destination. This should be used if there is a failure
   * in writing.
   */
  public void abort() {
    if (isClosed.get()) {
      return;
    }
    try {
      super.close();
    } catch (IOException ioe) {
      LOG.warn("Unable to abort file " + tmpFile, ioe);
    } finally {
      if (!tmpFile.delete()) {
        LOG.warn("Unable to delete tmp file during abort " + tmpFile);
      }
    }
  }
}
