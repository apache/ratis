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
import java.nio.channels.ClosedByInterruptException;
import java.util.Optional;

import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.server.metrics.SegmentedRaftLogMetrics;
import org.apache.ratis.util.IOUtils;
import org.apache.ratis.util.OpenCloseState;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.SizeInBytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.ratis.server.raftlog.RaftLog.INVALID_LOG_INDEX;
import static org.apache.ratis.server.raftlog.RaftLog.LEAST_VALID_LOG_INDEX;

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
  private final LogSegmentStartEnd startEnd;
  private final OpenCloseState state;
  private SegmentedRaftLogReader reader;
  private final SizeInBytes maxOpSize;
  private final SegmentedRaftLogMetrics raftLogMetrics;

  SegmentedRaftLogInputStream(File log, LogSegmentStartEnd startEnd,
      SizeInBytes maxOpSize, SegmentedRaftLogMetrics raftLogMetrics) {
    this.maxOpSize = maxOpSize;
    this.logFile = log;
    this.startEnd = startEnd;
    this.state = new OpenCloseState(getName());
    this.raftLogMetrics = raftLogMetrics;
  }

  private void init() throws IOException {
    state.open();
    boolean initSuccess = false;
    try {
      reader = new SegmentedRaftLogReader(logFile, maxOpSize, raftLogMetrics);
      initSuccess = reader.verifyHeader();
    } finally {
      if (!initSuccess) {
        if(reader != null) {
          reader.close();
          reader = null;
        }
        state.close();
      }
    }
  }

  String getName() {
    return logFile.getName();
  }

  public LogEntryProto nextEntry() throws IOException {
    if (state.isUnopened()) {
        try {
          init();
        } catch (Exception e) {
          if (e.getCause() instanceof ClosedByInterruptException) {
            LOG.warn("Initialization is interrupted: {}", this, e);
          } else {
            LOG.error("Failed to initialize {}", this, e);
          }
          throw IOUtils.asIOException(e);
        }
    }

    Preconditions.assertTrue(!state.isUnopened());
    if (state.isOpened()) {
        final LogEntryProto entry = reader.readEntry();
        if (entry != null) {
          long index = entry.getIndex();
          if (!startEnd.isOpen() && index >= startEnd.getEndIndex()) {
            /*
             * The end index may be derived from the segment recovery
             * process. It is possible that we still have some uncleaned garbage
             * in the end. We should skip them.
             */
            long skipAmt = logFile.length() - reader.getPos();
            if (skipAmt > 0) {
              LOG.info("Skipping {} bytes at the end of log '{}': reached entry {} out of [{}]",
                  skipAmt, getName(), index, startEnd);
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
   */
  static LogValidation scanEditLog(File file, long maxTxIdToScan, SizeInBytes maxOpSize)
      throws IOException {
    final LogSegmentStartEnd startEnd = LogSegmentStartEnd.valueOf(LEAST_VALID_LOG_INDEX);
    try(SegmentedRaftLogInputStream in = new SegmentedRaftLogInputStream(file, startEnd, maxOpSize, null)) {
      try {
        in.init();
      } catch (EOFException e) {
        LOG.warn("Invalid header for RaftLog segment {}", file, e);
        return new LogValidation(0, INVALID_LOG_INDEX, true);
      }
      return scanEditLog(in, maxTxIdToScan);
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
