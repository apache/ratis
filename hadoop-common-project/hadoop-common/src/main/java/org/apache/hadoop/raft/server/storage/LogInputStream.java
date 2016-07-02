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
import com.google.common.base.Throwables;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.raft.proto.RaftProtos.LogEntryProto;
import org.apache.hadoop.raft.server.storage.FileLogManager.LogValidation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;

import static org.apache.hadoop.raft.server.RaftConstants.INVALID_LOG_INDEX;

public class LogInputStream implements Closeable {
  static final Logger LOG = LoggerFactory.getLogger(LogInputStream.class);

  private enum State {
    UNINIT,
    OPEN,
    CLOSED
  }

  private final File logFile;
  private final long startIndex;
  private final long endIndex;
  private final boolean isInProgress;
  private State state = State.UNINIT;
  private LogReader reader;

  public LogInputStream(File log, long startIndex, long endIndex,
      boolean isInProgress) {
    this.logFile = log;
    this.startIndex = startIndex;
    this.endIndex = endIndex;
    this.isInProgress = isInProgress;
  }

  private void init() throws IOException {
    Preconditions.checkState(state == State.UNINIT);
    try {
      reader = new LogReader(logFile);
      // read the log header
      reader.readLogHeader();
      state = State.OPEN;
    } finally {
      if (reader == null) {
        state = State.CLOSED;
      }
    }
  }

  public long getStartIndex() {
    return startIndex;
  }

  public long getEndIndex() {
    return endIndex;
  }

  public String getName() {
    return logFile.getName();
  }

  private LogEntryProto nextEntry(boolean skipBrokenEntries) throws IOException {
    LogEntryProto entry = null;
    switch (state) {
      case UNINIT:
        try {
          init();
        } catch (Throwable e) {
          LOG.error("caught exception initializing " + this, e);
          if (skipBrokenEntries) {
            return null;
          }
          Throwables.propagateIfPossible(e, IOException.class);
        }
        Preconditions.checkState(state != State.UNINIT);
        return nextEntry(skipBrokenEntries);
      case OPEN:
        entry = reader.readEntry(skipBrokenEntries);
        if (entry != null) {
          long index = entry.getIndex();
          if ((index >= endIndex) && (endIndex != INVALID_LOG_INDEX)) {
            /**
             * The end index may be derived from the recoverUnfinalizedSegments
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
        break;
      case CLOSED:
        break; // return null
    }
    return entry;
  }

  long scanNextEntry() throws IOException {
    Preconditions.checkState(state == State.OPEN);
    return reader.scanEntry();
  }

  LogEntryProto nextEntry() throws IOException {
    return nextEntry(false);
  }

  protected LogEntryProto nextValidEntry() {
    try {
      return nextEntry(true);
    } catch (Throwable e) {
      LOG.error("nextValidEntry: got exception while reading " + this, e);
      return null;
    }
  }

  long getPosition() {
    if (state == State.OPEN) {
      return reader.getPos();
    } else {
      return 0;
    }
  }

  public void close() throws IOException {
    if (state == State.OPEN) {
      reader.close();
    }
    state = State.CLOSED;
  }

  public long length() throws IOException {
    // file size + size of both buffers
    return logFile.length();
  }

  public boolean isInProgress() {
    return isInProgress;
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
    LogInputStream in;
    try {
      in = new LogInputStream(file, INVALID_LOG_INDEX, INVALID_LOG_INDEX, false);
      // read the header, initialize the inputstream
      in.init();
    } catch (EOFException e) {
      LOG.warn("Log file " + file + " has no valid header", e);
      return new LogValidation(0, INVALID_LOG_INDEX, true);
    }

    try {
      return scanEditLog(in, maxTxIdToScan);
    } finally {
      IOUtils.closeStream(in);
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
  static LogValidation scanEditLog(LogInputStream in, long maxIndexToScan) {
    long lastPos = 0;
    long end = INVALID_LOG_INDEX;
    long numValid = 0;
    boolean hitError = false;
    while (end < maxIndexToScan) {
      long index;
      lastPos = in.getPosition();
      try {
        if (hitError) {
          LogEntryProto entry = in.nextValidEntry();
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
      } catch (Throwable t) {
        LOG.warn("Caught exception after scanning through {} ops from {}"
            + " while determining its valid length. Position was "
            + lastPos, numValid, in, t);
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
