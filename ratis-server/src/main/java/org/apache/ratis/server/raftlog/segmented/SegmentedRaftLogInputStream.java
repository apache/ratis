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
package org.apache.ratis.server.raftlog.segmented;

import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.Optional;

import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.server.metrics.SegmentedRaftLogMetrics;
import org.apache.ratis.util.IOUtils;
import org.apache.ratis.util.OpenCloseState;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.ratis.server.raftlog.RaftLog.INVALID_LOG_INDEX;

public class SegmentedRaftLogInputStream implements Closeable {
  static final Logger LOG = LoggerFactory.getLogger(SegmentedRaftLogInputStream.class);

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

  private final File logFile;
  private final long startIndex;
  private final long endIndex;
  private final boolean isOpen;
  private final OpenCloseState state;
  private SegmentedRaftLogReader reader;
  private final SegmentedRaftLogMetrics raftLogMetrics;

  public SegmentedRaftLogInputStream(File log, long startIndex, long endIndex, boolean isOpen) {
    this(log, startIndex, endIndex, isOpen, null);
  }

  SegmentedRaftLogInputStream(File log, long startIndex, long endIndex, boolean isOpen,
      SegmentedRaftLogMetrics raftLogMetrics) {
    if (isOpen) {
      Preconditions.assertTrue(endIndex == INVALID_LOG_INDEX);
    } else {
      Preconditions.assertTrue(endIndex >= startIndex);
    }

    this.logFile = log;
    this.startIndex = startIndex;
    this.endIndex = endIndex;
    this.isOpen = isOpen;
    this.state = new OpenCloseState(getName());
    this.raftLogMetrics = raftLogMetrics;
  }

  private void init() throws IOException {
    state.open();
    try {
      final SegmentedRaftLogReader r = new SegmentedRaftLogReader(logFile, raftLogMetrics);
      if (r.verifyHeader()) {
        reader = r;
      }
    } finally {
      if (reader == null) {
        state.close();
      }
    }
  }

  long getStartIndex() {
    return startIndex;
  }

  long getEndIndex() {
    return endIndex;
  }

  String getName() {
    return logFile.getName();
  }

  public LogEntryProto nextEntry() throws IOException {
    if (state.isUnopened()) {
        try {
          init();
        } catch (Exception e) {
          LOG.error("caught exception initializing " + this, e);
          throw IOUtils.asIOException(e);
        }
    }

    Preconditions.assertTrue(!state.isUnopened());
    if (state.isOpened()) {
        final LogEntryProto entry = reader.readEntry();
        if (entry != null) {
          long index = entry.getIndex();
          if (!isOpen() && index >= endIndex) {
            /*
             * The end index may be derived from the segment recovery
             * process. It is possible that we still have some uncleaned garbage
             * in the end. We should skip them.
             */
            long skipAmt = logFile.length() - reader.getPos();
            if (skipAmt > 0) {
              LOG.debug("skipping {} bytes at the end of log '{}': reached" +
                  " entry {} out of {}", skipAmt, getName(), index, endIndex);
              reader.skipFully(skipAmt);
            }
          }
        }
        return entry;
    } else if (state.isClosed()) {
      return null;
    }
    throw new IOException("Failed to get next entry from " + this, state.getThrowable());
  }

  long scanNextEntry() throws IOException {
    state.assertOpen();
    return reader.scanEntry();
  }

  long getPosition() {
    if (state.isOpened()) {
      return reader.getPos();
    } else {
      return 0;
    }
  }

  @Override
  public void close() throws IOException {
    if (state.close()) {
      Optional.ofNullable(reader).ifPresent(SegmentedRaftLogReader::close);
    }
  }

  boolean isOpen() {
    return isOpen;
  }

  @Override
  public String toString() {
    return getName();
  }

  /**
   * @param file          File being scanned and validated.
   * @param maxTxIdToScan Maximum Tx ID to try to scan.
   *                      The scan returns after reading this or a higher
   *                      ID. The file portion beyond this ID is
   *                      potentially being updated.
   * @return Result of the validation
   * @throws IOException
   */
  static LogValidation scanEditLog(File file, long maxTxIdToScan)
      throws IOException {
    SegmentedRaftLogInputStream in;
    try {
      in = new SegmentedRaftLogInputStream(file, INVALID_LOG_INDEX, INVALID_LOG_INDEX, false, null);
      // read the header, initialize the inputstream
      in.init();
    } catch (EOFException e) {
      LOG.warn("Log file " + file + " has no valid header", e);
      return new LogValidation(0, INVALID_LOG_INDEX, true);
    }

    try {
      return scanEditLog(in, maxTxIdToScan);
    } finally {
      IOUtils.cleanup(LOG, in);
    }
  }

  /**
   * Find the last valid entry index in the stream.
   * If there are invalid or corrupt entries in the middle of the stream,
   * scanEditLog will skip over them.
   *
   * This reads through the stream but does not close it.
   *
   * @param maxIndexToScan Maximum entry index to try to scan. The scan returns
   *                       after reading this or a higher index. The file
   *                       portion beyond this index is potentially being
   *                       updated.
   */
  static LogValidation scanEditLog(SegmentedRaftLogInputStream in, long maxIndexToScan) {
    long lastPos = 0;
    long end = INVALID_LOG_INDEX;
    long numValid = 0;
    boolean hitError = false;
    while (end < maxIndexToScan) {
      long index;
      lastPos = in.getPosition();
      try {
        if (hitError) {
          LogEntryProto entry = in.nextEntry();
          index = entry != null ? entry.getIndex() : INVALID_LOG_INDEX;
          LOG.warn("After resync, position is " + in.getPosition());
        } else {
          index = in.scanNextEntry();
        }
        if (index == INVALID_LOG_INDEX) {
          break;
        } else {
          hitError = false;
        }
      } catch (Exception e) {
        LOG.warn("Caught exception after scanning through {} ops from {}"
            + " while determining its valid length. Position was "
            + lastPos, numValid, in, e);
        hitError = true;
        continue;
      }
      if (end == INVALID_LOG_INDEX || index > end) {
        end = index;
      }
      numValid++;
    }
    return new LogValidation(lastPos, end, false);
  }
}
