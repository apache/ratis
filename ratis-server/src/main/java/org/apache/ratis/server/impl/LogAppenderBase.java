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
package org.apache.ratis.server.impl;

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotReplyProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotRequestProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotResult;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.leader.FollowerInfo;
import org.apache.ratis.server.leader.LeaderState;
import org.apache.ratis.server.leader.LogAppender;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.raftlog.RaftLog.EntryWithData;
import org.apache.ratis.server.raftlog.RaftLogIOException;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.util.DataQueue;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.SizeInBytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import static org.apache.ratis.server.impl.RaftServerConstants.DEFAULT_CALLID;
import static org.apache.ratis.server.metrics.RaftLogMetrics.LOG_APPENDER_INSTALL_SNAPSHOT_METRIC;

/**
 * A daemon thread appending log entries to a follower peer.
 */
public class LogAppenderBase implements LogAppender {
  public static final Logger LOG = LoggerFactory.getLogger(LogAppenderBase.class);

  private final String name;
  private final RaftServer.Division server;
  private final LeaderState leaderState;
  private final FollowerInfo follower;

  private final DataQueue<EntryWithData> buffer;
  private final int snapshotChunkMaxSize;

  private final LogAppenderDaemon daemon;

  public LogAppenderBase(RaftServer.Division server, LeaderState leaderState, FollowerInfo f) {
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
  public AppendEntriesRequestProto newAppendEntriesRequest(long callId, boolean heartbeat) throws RaftLogIOException {
    final TermIndex previous = getPrevious(follower.getNextIndex());
    final long snapshotIndex = follower.getSnapshotIndex();
    final long heartbeatRemainingMs = getHeartbeatRemainingTime();
    if (heartbeatRemainingMs <= 0L || heartbeat) {
      // heartbeat
      return leaderState.newAppendEntriesRequestProto(follower, Collections.emptyList(), previous, callId);
    }

    Preconditions.assertTrue(buffer.isEmpty(), () -> "buffer has " + buffer.getNumElements() + " elements.");

    final long leaderNext = getRaftLog().getNextIndex();
    final long followerNext = follower.getNextIndex();
    final long halfMs = heartbeatRemainingMs/2;
    for (long next = followerNext; leaderNext > next && getHeartbeatRemainingTime() - halfMs > 0; ) {
      if (!buffer.offer(getRaftLog().getEntryWithData(next++))) {
        break;
      }
    }
    if (buffer.isEmpty()) {
      return null;
    }

    final List<LogEntryProto> protos = buffer.pollList(getHeartbeatRemainingTime(), EntryWithData::getEntry,
        (entry, time, exception) -> LOG.warn("{}: Failed to get {} in {}: {}",
            follower.getName(), entry, time, exception));
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

  /** Send an appendEntries RPC; retry indefinitely. */
  private AppendEntriesReplyProto sendAppendEntriesWithRetries()
      throws InterruptedException, InterruptedIOException, RaftLogIOException {
    int retry = 0;
    AppendEntriesRequestProto request = null;
    while (isRunning()) { // keep retrying for IOException
      try {
        if (request == null || request.getEntriesCount() == 0) {
          request = newAppendEntriesRequest(DEFAULT_CALLID, false);
        }

        if (request == null) {
          LOG.trace("{} no entries to send now, wait ...", this);
          return null;
        } else if (!isRunning()) {
          LOG.info("{} is stopped. Skip appendEntries.", this);
          return null;
        }

        follower.updateLastRpcSendTime();
        final AppendEntriesReplyProto r = getServerRpc().appendEntries(request);
        follower.updateLastRpcResponseTime();

        getLeaderState().onFollowerCommitIndex(getFollower(), r.getFollowerCommit());
        return r;
      } catch (InterruptedIOException | RaftLogIOException e) {
        throw e;
      } catch (IOException ioe) {
        // TODO should have more detailed retry policy here.
        if (retry++ % 10 == 0) { // to reduce the number of messages
          LOG.warn("{}: Failed to appendEntries (retry={}): {}", this, retry++, ioe);
        }
        handleException(ioe);
      }
      if (isRunning()) {
        server.properties().rpcSleepTime().sleep();
      }
    }
    return null;
  }

  @Override
  public InstallSnapshotRequestProto newInstallSnapshotNotificationRequest(TermIndex firstAvailableLogTermIndex) {
    Preconditions.assertTrue(firstAvailableLogTermIndex.getIndex() > 0);
    synchronized (server) {
      return ServerProtoUtils.toInstallSnapshotRequestProto(server.getMemberId(), getFollowerId(),
          server.getInfo().getCurrentTerm(), firstAvailableLogTermIndex, server.getRaftConf());
    }
  }

  @Override
  public Iterable<InstallSnapshotRequestProto> newInstallSnapshotRequests(String requestId, SnapshotInfo snapshot) {
    return ServerImplUtils.newInstallSnapshotRequests(server, getFollowerId(), requestId,
        snapshot, snapshotChunkMaxSize);
  }

  private InstallSnapshotReplyProto installSnapshot(SnapshotInfo snapshot) throws InterruptedIOException {
    String requestId = UUID.randomUUID().toString();
    InstallSnapshotReplyProto reply = null;
    try {
      for (InstallSnapshotRequestProto request : newInstallSnapshotRequests(requestId, snapshot)) {
        follower.updateLastRpcSendTime();
        reply = getServerRpc().installSnapshot(request);
        follower.updateLastRpcResponseTime();

        if (!reply.getServerReply().getSuccess()) {
          return reply;
        }
      }
    } catch (InterruptedIOException iioe) {
      throw iioe;
    } catch (Exception ioe) {
      LOG.warn("{}: Failed to installSnapshot {}: {}", this, snapshot, ioe);
      handleException(ioe);
      return null;
    }

    if (reply != null) {
      follower.setSnapshotIndex(snapshot.getTermIndex().getIndex());
      LOG.info("{}: installSnapshot {} successfully", this, snapshot);
      server.getRaftServerMetrics().getCounter(LOG_APPENDER_INSTALL_SNAPSHOT_METRIC).inc();
    }
    return reply;
  }

  @Override
  public void run() throws InterruptedException, IOException {
    while (isRunning()) {
      if (shouldSendRequest()) {
        SnapshotInfo snapshot = shouldInstallSnapshot();
        if (snapshot != null) {
          LOG.info("{}: followerNextIndex = {} but logStartIndex = {}, send snapshot {} to follower",
              this, follower.getNextIndex(), getRaftLog().getStartIndex(), snapshot);

          final InstallSnapshotReplyProto r = installSnapshot(snapshot);
          if (r != null && r.getResult() == InstallSnapshotResult.NOT_LEADER) {
            checkResponseTerm(r.getTerm());
          } // otherwise if r is null, retry the snapshot installation
        } else {
          final AppendEntriesReplyProto r = sendAppendEntriesWithRetries();
          if (r != null) {
            handleReply(r);
          }
        }
      }
      if (isRunning() && !shouldAppendEntries(follower.getNextIndex())) {
        final long waitTime = getHeartbeatRemainingTime();
        if (waitTime > 0) {
          synchronized (this) {
            wait(waitTime);
          }
        }
      }
      getLeaderState().checkHealth(getFollower());
    }
  }

  private void handleReply(AppendEntriesReplyProto reply) throws IllegalArgumentException {
    if (reply != null) {
      switch (reply.getResult()) {
        case SUCCESS:
          final long oldNextIndex = follower.getNextIndex();
          final long nextIndex = reply.getNextIndex();
          if (nextIndex < oldNextIndex) {
            throw new IllegalStateException("nextIndex=" + nextIndex
                + " < oldNextIndex=" + oldNextIndex
                + ", reply=" + ServerProtoUtils.toString(reply));
          }

          if (nextIndex > oldNextIndex) {
            follower.updateMatchIndex(nextIndex - 1);
            follower.increaseNextIndex(nextIndex);
            getLeaderState().onFollowerSuccessAppendEntries(getFollower());
          }
          break;
        case NOT_LEADER:
          // check if should step down
          checkResponseTerm(reply.getTerm());
          break;
        case INCONSISTENCY:
          follower.decreaseNextIndex(reply.getNextIndex());
          break;
        case UNRECOGNIZED:
          LOG.warn("{}: received {}", this, reply.getResult());
          break;
        default: throw new IllegalArgumentException("Unable to process result " + reply.getResult());
      }
    }
  }

  private void handleException(Exception e) {
    LOG.trace("TRACE", e);
    getServerRpc().handleException(getFollowerId(), e, false);
  }
}
