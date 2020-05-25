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
package org.apache.ratis.logservice.server;

import static java.util.Objects.requireNonNull;

import java.util.NoSuchElementException;

import org.apache.ratis.logservice.proto.LogServiceProtos.AppendLogEntryRequestProto;
import org.apache.ratis.logservice.proto.LogServiceProtos.LogServiceRequestProto;
import org.apache.ratis.logservice.proto.LogServiceProtos.LogServiceRequestProto.RequestCase;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.raftlog.RaftLogIOException;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.thirdparty.com.google.protobuf.TextFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A reader for the {@link RaftLog} which is accessed using LogService recordId's instead
 * of Raft log indexes. Not thread-safe.
 */
public class LogServiceRaftLogReader implements  RaftLogReader{
  private static final Logger LOG = LoggerFactory.getLogger(LogServiceRaftLogReader.class);
  private final RaftLog raftLog;

  private long currentRecordId = -1;
  private long currentRaftIndex = -1;
  private AppendLogEntryRequestProto currentLogEntry = null;
  private int currentLogEntryOffset = -1;
  private ByteString currentRecord = null;

  public LogServiceRaftLogReader(RaftLog raftLog) {
    this.raftLog = requireNonNull(raftLog);
  }

  /**
   * Positions this reader just before the current recordId. Use {@link #next()} to get that
   * element, but take care to check if a value is present using {@link #hasNext()} first.
   */
  @Override
  public void seek(long recordId) throws RaftLogIOException, InvalidProtocolBufferException {
    LOG.trace("Seeking to recordId={}", recordId);
    // RaftLog starting index
    currentRaftIndex = raftLog.getStartIndex();
    currentRecordId = 0;

    currentLogEntry = null;
    currentLogEntryOffset = -1;
    currentRecord = null;

    loadNext();
    while (currentRecordId < recordId && hasNext()) {
      next();
      currentRecordId++;
    }
  }

  /**
   * Returns true if there is a log entry to read.
   */
  @Override
  public boolean hasNext() throws RaftLogIOException, InvalidProtocolBufferException {
    return currentRecord != null;
  }

  /**
   * Returns the next log entry. Ensure {@link #hasNext()} returns true before
   * calling this method.
   */
  @Override
  public byte[] next() throws RaftLogIOException, InvalidProtocolBufferException {
    if (currentRecord == null) {
      throw new NoSuchElementException();
    }
    ByteString current = currentRecord;
    currentRecord = null;
    loadNext();
    return current.toByteArray();
  }

  /**
   * Finds the next record from the RaftLog and sets it as {@link #currentRecord}.
   */
  private void loadNext() throws RaftLogIOException, InvalidProtocolBufferException {
    // Clear the old "current" record
    currentRecord = null;
    if (LOG.isTraceEnabled()) {
      LOG.trace("Loading next value: raftIndex={}, recordId={}, proto='{}', offset={}",
          currentRaftIndex, currentRecordId,
          currentLogEntry == null ? "null" : TextFormat.shortDebugString(currentLogEntry),
              currentLogEntryOffset);
    }
    // Continue iterating over the current entry.
    if (currentLogEntry != null) {
      assert currentLogEntryOffset != -1;
      currentLogEntryOffset++;

      // We have an element to read from our current entry
      if (currentLogEntryOffset < currentLogEntry.getDataCount()) {
        currentRecord = currentLogEntry.getData(currentLogEntryOffset);
        return;
      }
      // We don't have an element in our current entry so null it out.
      currentLogEntry = null;
      currentLogEntryOffset = -1;
      // Also, increment to the next element in the RaftLog
      currentRaftIndex++;
    }

    // Make sure we don't read off the end of the Raft log
    for (; currentRaftIndex <= raftLog.getLastCommittedIndex(); currentRaftIndex++) {
      try {
        LogEntryProto entry = raftLog.get(currentRaftIndex);
        if (LOG.isTraceEnabled()) {
          LOG.trace("Raft Index: {} Entry: {}", currentRaftIndex,
              TextFormat.shortDebugString(entry));
        }
        if (entry == null || entry.hasConfigurationEntry()) {
          continue;
        }

        LogServiceRequestProto logServiceProto =
            LogServiceRequestProto.parseFrom(entry.getStateMachineLogEntry().getLogData());
        // TODO is it possible to get LogService messages that aren't appends?
        if (RequestCase.APPENDREQUEST != logServiceProto.getRequestCase()) {
          continue;
        }

        currentLogEntry = logServiceProto.getAppendRequest();
        currentLogEntryOffset = 0;
        if (currentLogEntry.getDataCount() > 0) {
          currentRecord = currentLogEntry.getData(currentLogEntryOffset);
          return;
        }
        currentLogEntry = null;
        currentLogEntryOffset = -1;
      } catch (RaftLogIOException e) {
        LOG.error("Caught exception reading from RaftLog", e);
        throw e;
      } catch (InvalidProtocolBufferException e) {
        LOG.error("Caught exception reading LogService protobuf from RaftLog", e);
        throw e;
      }
    }
    // If we make it here, we've read off the end of the RaftLog.
  }

  @Override
  public long getCurrentRaftIndex(){
    return currentRaftIndex;
  }
}
