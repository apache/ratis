/**
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
package org.apache.hadoop.raft.server.storage;

import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.raft.server.RaftConstants;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Comparator;

import static org.apache.hadoop.raft.server.RaftConstants.INVALID_LOG_INDEX;

class FileLogManager implements Closeable {

  /**
   * Format the underlying storage, removing any previously stored data.
   */
  void format() throws IOException{

  }

  /**
   * Begin writing to a new segment of the log stream, which starts at
   * the given transaction ID.
   */
  LogOutputStream startLogSegment(long startIndex) throws IOException {
    return null;
  }

  /**
   * Mark the log segment that spans from startIndex to endIndex as
   * finalized and complete.
   */
  void finalizeLogSegment(long startIndex, long endIndex) throws IOException {
  }

  /**
   * Recover segments which have not been finalized.
   */
  void recoverUnfinalizedSegments() throws IOException {

  }

  /**
   * Discard the segments whose starting index is >= the given index.
   * @param startIndex The given index should be right at the segment boundary,
   *                   i.e., it should be the start index of some segment,
   *                   if segment corresponding to the index exists.
   */
  void discardSegments(long startIndex) throws IOException {

  }

  @Override
  public void close() throws IOException {
  }

  public static class LogFile {
    private File file;
    private final long startIndex;
    private long endIndex;

    private boolean hasCorruptHeader = false;
    private final boolean isInProgress;

    final static Comparator<LogFile> LOG_FILE_COMPARATOR
        = (a, b) -> ComparisonChain.start()
            .compare(a.getStartIndex(), b.getStartIndex())
            .compare(a.getEndIndex(), b.getEndIndex())
            .result();

    LogFile(File file, long startIndex, long endIndex) {
      this(file, startIndex, endIndex, false);
    }

    LogFile(File file, long startIndex, long endIndex, boolean isInProgress) {
      Preconditions.checkArgument(
          (endIndex == INVALID_LOG_INDEX && isInProgress) ||
          (endIndex != INVALID_LOG_INDEX && endIndex >= startIndex));
      Preconditions.checkArgument(!isInProgress || endIndex == INVALID_LOG_INDEX);

      Preconditions.checkArgument(startIndex > 0);
      Preconditions.checkArgument(file != null);

      this.startIndex = startIndex;
      this.endIndex = endIndex;
      this.file = file;
      this.isInProgress = isInProgress;
    }

    public long getStartIndex() {
      return startIndex;
    }

    public long getEndIndex() {
      return endIndex;
    }

    boolean containsIndex(long index) {
      return startIndex <= index && index <= endIndex;
    }

    /**
     * Find out where the edit log ends.
     * This will update the lastTxId of the EditLogFile or
     * mark it as corrupt if it is.
     * @param maxTxIdToScan Maximum Tx ID to try to scan.
     *                      The scan returns after reading this or a higher ID.
     *                      The file portion beyond this ID is potentially being
     *                      updated.
     */
    public void scanLog(long maxTxIdToScan)
        throws IOException {
      LogValidation val = LogInputStream.scanEditLog(file, maxTxIdToScan);
      this.endIndex = val.getEndIndex();
      this.hasCorruptHeader = val.hasCorruptHeader();
    }

    public boolean isInProgress() {
      return isInProgress;
    }

    public File getFile() {
      return file;
    }

    boolean hasCorruptHeader() {
      return hasCorruptHeader;
    }

    void moveAsideCorruptFile() throws IOException {
      Preconditions.checkState(hasCorruptHeader);
      renameSelf(".corrupt");
    }

    void moveAsideTrashFile(long markerTxid) throws IOException {
      assert this.getStartIndex() >= markerTxid;
      renameSelf(".trash");
    }

    public void moveAsideEmptyFile() throws IOException {
      Preconditions.checkState(endIndex == RaftConstants.INVALID_LOG_INDEX);
      renameSelf(".empty");
    }

    private void renameSelf(String newSuffix) throws IOException {
      File src = file;
      File dst = new File(src.getParent(), src.getName() + newSuffix);
      // renameTo fails on Windows if the destination file already exists.
      try {
        if (dst.exists()) {
          if (!dst.delete()) {
            throw new IOException("Couldn't delete " + dst);
          }
        }
        NativeIO.renameTo(src, dst);
      } catch (IOException e) {
        throw new IOException(
            "Couldn't rename log " + src + " to " + dst, e);
      }
      file = dst;
    }

    @Override
    public String toString() {
      return String.format("EditLogFile(file=%s,first=%019d,last=%019d,"
              +"inProgress=%b,hasCorruptHeader=%b)",
          file.toString(), startIndex, endIndex,
          isInProgress(), hasCorruptHeader);
    }
  }

  static class LogValidation {
    private final long validLength;
    private final long endIndex;
    private final boolean hasCorruptHeader;

    LogValidation(long validLength, long endIndex, boolean hasCorruptHeader) {
      this.validLength = validLength;
      this.endIndex = endIndex;
      this.hasCorruptHeader = hasCorruptHeader;
    }

    long getValidLength() {
      return validLength;
    }

    long getEndIndex() {
      return endIndex;
    }

    boolean hasCorruptHeader() {
      return hasCorruptHeader;
    }
  }
}
