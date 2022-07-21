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
package org.apache.ratis.server.leader;

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotRequestProto;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.raftlog.RaftLog.EntryWithData;
import org.apache.ratis.server.raftlog.RaftLogIOException;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.util.AwaitForSignal;
import org.apache.ratis.util.DataQueue;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.SizeInBytes;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * An abstract implementation of {@link LogAppender}.
 */
public abstract class LogAppenderBase implements LogAppender {
  private final String name;
  private final RaftServer.Division server;
  private final LeaderState leaderState;
  private final FollowerInfo follower;

  private final DataQueue<EntryWithData> buffer;
  private final int snapshotChunkMaxSize;

  private final LogAppenderDaemon daemon;
  private final AwaitForSignal eventAwaitForSignal;

  protected LogAppenderBase(RaftServer.Division server, LeaderState leaderState, FollowerInfo f) {
    this.follower = f;
    this.name = follower.getName() + "-" + JavaUtils.getClassSimpleName(getClass());
    this.server = server;
    this.leaderState = leaderState;

    final RaftProperties properties = server.getRaftServer().getProperties();
    this.snapshotChunkMaxSize = RaftServerConfigKeys.Log.Appender.snapshotChunkSizeMax(properties).getSizeInt();

    final SizeInBytes bufferByteLimit = RaftServerConfigKeys.Log.Appender.bufferByteLimit(properties);
    final int bufferElementLimit = RaftServerConfigKeys.Log.Appender.bufferElementLimit(properties);
    this.buffer = new DataQueue<>(this, bufferByteLimit, bufferElementLimit, EntryWithData::getSerializedSize);
    this.daemon = new LogAppenderDaemon(this);
    this.eventAwaitForSignal = new AwaitForSignal(name);
  }

  @Override
  public AwaitForSignal getEventAwaitForSignal() {
    return eventAwaitForSignal;
  }

  @Override
  public final RaftServer.Division getServer() {
    return server;
  }

  @Override
  public String toString() {
    return name;
  }

  @Override
  public void start() {
    daemon.tryToStart();
  }

  @Override
  public boolean isRunning() {
    return daemon.isWorking();
  }

  @Override
  public void stop() {
    daemon.tryToClose();
  }

  @Override
  public final FollowerInfo getFollower() {
    return follower;
  }

  @Override
  public final LeaderState getLeaderState() {
    return leaderState;
  }

  public boolean hasPendingDataRequests() {
    return false;
  }

  private TermIndex getPrevious(long nextIndex) {
    if (nextIndex == RaftLog.LEAST_VALID_LOG_INDEX) {
      return null;
    }

    final long previousIndex = nextIndex - 1;
    final TermIndex previous = getRaftLog().getTermIndex(previousIndex);
    if (previous != null) {
      return previous;
    }

    final SnapshotInfo snapshot = server.getStateMachine().getLatestSnapshot();
    if (snapshot != null) {
      final TermIndex snapshotTermIndex = snapshot.getTermIndex();
      if (snapshotTermIndex.getIndex() == previousIndex) {
        return snapshotTermIndex;
      }
    }

    return null;
  }

  @Override
  public AppendEntriesRequestProto newAppendEntriesRequest(long callId, boolean heartbeat)
      throws RaftLogIOException {
    final long heartbeatWaitTimeMs = getHeartbeatWaitTimeMs();
    final TermIndex previous = getPrevious(follower.getNextIndex());
    if (heartbeatWaitTimeMs <= 0L || heartbeat) {
      // heartbeat
      return leaderState.newAppendEntriesRequestProto(follower, Collections.emptyList(),
          hasPendingDataRequests()? null : previous, callId);
    }

    Preconditions.assertTrue(buffer.isEmpty(), () -> "buffer has " + buffer.getNumElements() + " elements.");

    final long snapshotIndex = follower.getSnapshotIndex();
    final long leaderNext = getRaftLog().getNextIndex();
    final long followerNext = follower.getNextIndex();
    final long halfMs = heartbeatWaitTimeMs/2;
    for (long next = followerNext; leaderNext > next && getHeartbeatWaitTimeMs() - halfMs > 0; ) {
      if (!buffer.offer(getRaftLog().getEntryWithData(next++))) {
        break;
      }
    }
    if (buffer.isEmpty()) {
      return null;
    }

    final List<LogEntryProto> protos = buffer.pollList(getHeartbeatWaitTimeMs(), EntryWithData::getEntry,
        (entry, time, exception) -> LOG.warn("Failed to get {} in {}: {}", entry, time, exception));
    buffer.clear();
    assertProtos(protos, followerNext, previous, snapshotIndex);
    return leaderState.newAppendEntriesRequestProto(follower, protos, previous, callId);
  }

  private void assertProtos(List<LogEntryProto> protos, long nextIndex, TermIndex previous, long snapshotIndex) {
    if (protos.isEmpty()) {
      return;
    }
    final long firstIndex = protos.get(0).getIndex();
    Preconditions.assertTrue(firstIndex == nextIndex,
        () -> follower.getName() + ": firstIndex = " + firstIndex + " != nextIndex = " + nextIndex);
    if (firstIndex > RaftLog.LEAST_VALID_LOG_INDEX) {
      // Check if nextIndex is 1 greater than the snapshotIndex. If yes, then
      // we do not have to check for the existence of previous.
      if (nextIndex != snapshotIndex + 1) {
        Objects.requireNonNull(previous,
            () -> follower.getName() + ": Previous TermIndex not found for firstIndex = " + firstIndex);
        Preconditions.assertTrue(previous.getIndex() == firstIndex - 1,
            () -> follower.getName() + ": Previous = " + previous + " but firstIndex = " + firstIndex);
      }
    }
  }

  @Override
  public InstallSnapshotRequestProto newInstallSnapshotNotificationRequest(TermIndex firstAvailableLogTermIndex) {
    Preconditions.assertTrue(firstAvailableLogTermIndex.getIndex() >= 0);
    synchronized (server) {
      return LeaderProtoUtils.toInstallSnapshotRequestProto(server, getFollowerId(), firstAvailableLogTermIndex);
    }
  }

  @Override
  public Iterable<InstallSnapshotRequestProto> newInstallSnapshotRequests(String requestId, SnapshotInfo snapshot) {
    return new InstallSnapshotRequests(server, getFollowerId(), requestId, snapshot, snapshotChunkMaxSize);
  }
}
