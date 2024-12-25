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
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.ReferenceCountedObject;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongUnaryOperator;

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

  private final AtomicBoolean heartbeatTrigger = new AtomicBoolean();
  private final TimeDuration waitTimeMin;

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

    this.waitTimeMin = RaftServerConfigKeys.Log.Appender.waitTimeMin(properties);
  }

  @Override
  public void triggerHeartbeat() {
    if (heartbeatTrigger.compareAndSet(false, true)) {
      notifyLogAppender();
    }
  }

  protected void resetHeartbeatTrigger() {
    heartbeatTrigger.set(false);
  }

  @Override
  public boolean shouldSendAppendEntries() {
    return heartbeatTrigger.get() || LogAppender.super.shouldSendAppendEntries();
  }

  @Override
  public long getHeartbeatWaitTimeMs() {
    return heartbeatTrigger.get() ? 0 :
        LogAppender.super.getHeartbeatWaitTimeMs();
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
    return daemon.isWorking() && server.getInfo().isLeader();
  }

  @Override
  public CompletableFuture<LifeCycle.State> stopAsync() {
    return daemon.tryToClose();
  }

  void restart() {
    if (!server.getInfo().isAlive()) {
      LOG.warn("Failed to restart {}: server {} is not alive", this, server.getMemberId());
      return;
    }
    getLeaderState().restart(this);
  }

  protected TimeDuration getWaitTimeMin() {
    return waitTimeMin;
  }

  protected TimeDuration getRemainingWaitTime() {
    return waitTimeMin.add(getFollower().getLastRpcSendTime().elapsedTime().negate());
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

  protected long getNextIndexForInconsistency(long requestFirstIndex, long replyNextIndex) {
    long next = replyNextIndex;
    final long i = getFollower().getMatchIndex() + 1;
    if (i > next && i != requestFirstIndex) {
      // Ideally, we should set nextIndex to a value greater than matchIndex.
      // However, we must not resend the same first entry due to some special cases (e.g. the log is empty).
      // Otherwise, the follower will reply INCONSISTENCY again.
      next = i;
    }
    if (next == requestFirstIndex && next > RaftLog.LEAST_VALID_LOG_INDEX) {
      // Avoid resending the same first entry.
      next--;
    }
    return next;
  }

  protected LongUnaryOperator getNextIndexForError(long newNextIndex) {
    return oldNextIndex -> {
      final long m = getFollower().getMatchIndex() + 1;
      final long n = oldNextIndex <= 0L ? oldNextIndex : Math.min(oldNextIndex - 1, newNextIndex);
      if (m > n) {
        if (m > newNextIndex) {
          LOG.info("Set nextIndex to matchIndex + 1 (= " + m + ")");
        }
        return m;
      } else if (oldNextIndex <= 0L) {
        return oldNextIndex; // no change.
      } else {
        LOG.info("Decrease nextIndex to " + n);
        return n;
      }
    };
  }

  @Override
  public AppendEntriesRequestProto newAppendEntriesRequest(long callId, boolean heartbeat) {
    throw new UnsupportedOperationException("Use nextAppendEntriesRequest(" + callId + ", " + heartbeat +") instead.");
  }

/**
 * Create a {@link AppendEntriesRequestProto} object using the {@link FollowerInfo} of this {@link LogAppender}.
 * The {@link AppendEntriesRequestProto} object may contain zero or more log entries.
 * When there is zero log entries, the {@link AppendEntriesRequestProto} object is a heartbeat.
 *
 * @param callId The call id of the returned request.
 * @param heartbeat the returned request must be a heartbeat.
 *
 * @return a retained reference of {@link AppendEntriesRequestProto} object.
 *         Since the returned reference is retained, the caller must call {@link ReferenceCountedObject#release()}}
 *         after use.
 */
  protected ReferenceCountedObject<AppendEntriesRequestProto> nextAppendEntriesRequest(long callId, boolean heartbeat)
      throws RaftLogIOException {
    final long heartbeatWaitTimeMs = getHeartbeatWaitTimeMs();
    final TermIndex previous = getPrevious(follower.getNextIndex());
    if (heartbeatWaitTimeMs <= 0L || heartbeat) {
      // heartbeat
      AppendEntriesRequestProto heartbeatRequest =
          leaderState.newAppendEntriesRequestProto(follower, Collections.emptyList(),
              hasPendingDataRequests() ? null : previous, callId);
      ReferenceCountedObject<AppendEntriesRequestProto> ref = ReferenceCountedObject.wrap(heartbeatRequest);
      ref.retain();
      return ref;
    }

    Preconditions.assertTrue(buffer.isEmpty(), () -> "buffer has " + buffer.getNumElements() + " elements.");

    final long snapshotIndex = follower.getSnapshotIndex();
    final long leaderNext = getRaftLog().getNextIndex();
    final long followerNext = follower.getNextIndex();
    final long halfMs = heartbeatWaitTimeMs/2;
    final Map<Long, ReferenceCountedObject<EntryWithData>> offered = new HashMap<>();
    for (long next = followerNext; leaderNext > next && getHeartbeatWaitTimeMs() - halfMs > 0; next++) {
      final ReferenceCountedObject<EntryWithData> entryWithData;
      try {
        entryWithData = getRaftLog().retainEntryWithData(next);
        if (!buffer.offer(entryWithData.get())) {
          entryWithData.release();
          break;
        }
        offered.put(next, entryWithData);
      } catch (Exception e){
        for (ReferenceCountedObject<EntryWithData> ref : offered.values()) {
          ref.release();
        }
        offered.clear();
        throw e;
      }
    }
    if (buffer.isEmpty()) {
      return null;
    }

    final List<LogEntryProto> protos;
    try {
      protos = buffer.pollList(getHeartbeatWaitTimeMs(), EntryWithData::getEntry,
          (entry, time, exception) -> LOG.warn("Failed to get {} in {}",
              entry, time.toString(TimeUnit.MILLISECONDS, 3), exception));
    } catch (RaftLogIOException e) {
      for (ReferenceCountedObject<EntryWithData> ref : offered.values()) {
        ref.release();
      }
      offered.clear();
      throw e;
    } finally {
      for (EntryWithData entry : buffer) {
        // Release remaining entries.
        Optional.ofNullable(offered.remove(entry.getIndex())).ifPresent(ReferenceCountedObject::release);
      }
      buffer.clear();
    }
    assertProtos(protos, followerNext, previous, snapshotIndex);
    AppendEntriesRequestProto appendEntriesProto =
        leaderState.newAppendEntriesRequestProto(follower, protos, previous, callId);
    return ReferenceCountedObject.delegateFrom(offered.values(), appendEntriesProto);
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
