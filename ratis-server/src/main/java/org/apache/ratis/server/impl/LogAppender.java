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
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerRpc;
import org.apache.ratis.server.leader.FollowerInfo;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.leader.LeaderState;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.raftlog.RaftLog.EntryWithData;
import org.apache.ratis.server.raftlog.RaftLogIOException;
import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.*;

import static org.apache.ratis.server.impl.RaftServerConstants.DEFAULT_CALLID;
import static org.apache.ratis.server.metrics.RaftLogMetrics.LOG_APPENDER_INSTALL_SNAPSHOT_METRIC;
import static org.apache.ratis.util.LifeCycle.State.CLOSED;
import static org.apache.ratis.util.LifeCycle.State.CLOSING;
import static org.apache.ratis.util.LifeCycle.State.EXCEPTION;
import static org.apache.ratis.util.LifeCycle.State.NEW;
import static org.apache.ratis.util.LifeCycle.State.RUNNING;
import static org.apache.ratis.util.LifeCycle.State.STARTING;

/**
 * A daemon thread appending log entries to a follower peer.
 */
public class LogAppender {
  public static final Logger LOG = LoggerFactory.getLogger(LogAppender.class);

  class AppenderDaemon {
    private final String name = LogAppender.this + "-" + JavaUtils.getClassSimpleName(getClass());
    private final LifeCycle lifeCycle = new LifeCycle(name);
    private final Daemon daemon = new Daemon(this::run);

    void start() {
      // The life cycle state could be already closed due to server shutdown.
      synchronized (lifeCycle) {
        if (lifeCycle.compareAndTransition(NEW, STARTING)) {
          daemon.start();
        }
      }
    }

    void run() {
      synchronized (lifeCycle) {
        if (!isRunning()) {
          return;
        }
        transitionLifeCycle(RUNNING);
      }
      try {
        runAppenderImpl();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.info(this + " was interrupted: " + e);
      } catch (InterruptedIOException e) {
        LOG.info(this + " was interrupted: " + e);
      } catch (RaftLogIOException e) {
        LOG.error(this + " failed RaftLog", e);
        transitionLifeCycle(EXCEPTION);
      } catch (IOException e) {
        LOG.error(this + " failed IOException", e);
        transitionLifeCycle(EXCEPTION);
      } catch (Throwable e) {
        LOG.error(this + " unexpected exception", e);
        transitionLifeCycle(EXCEPTION);
      } finally {
        synchronized (lifeCycle) {
          if (!lifeCycle.compareAndTransition(CLOSING, CLOSED)) {
            lifeCycle.transitionIfNotEqual(EXCEPTION);
          }
          if (lifeCycle.getCurrentState() == EXCEPTION) {
            leaderState.restart(LogAppender.this);
          }
        }
      }
    }

    boolean isRunning() {
      return !LifeCycle.States.CLOSING_OR_CLOSED_OR_EXCEPTION.contains(lifeCycle.getCurrentState());
    }

    void stop() {
      synchronized (lifeCycle) {
        if (LifeCycle.States.CLOSING_OR_CLOSED.contains(lifeCycle.getCurrentState())) {
          return;
        }
        if (lifeCycle.compareAndTransition(NEW, CLOSED)) {
          return;
        }
        transitionLifeCycle(CLOSING);
      }
      daemon.interrupt();
    }

    @Override
    public String toString() {
      return name;
    }

    private boolean transitionLifeCycle(LifeCycle.State to) {
      synchronized (lifeCycle) {
        if (LifeCycle.State.isValid(lifeCycle.getCurrentState(), to)) {
          lifeCycle.transition(to);
          return true;
        }
        return false;
      }
    }
  }

  private final String name;
  private final RaftServer.Division server;
  private final LeaderState leaderState;
  private final RaftLog raftLog;
  private final FollowerInfo follower;

  private final DataQueue<EntryWithData> buffer;
  private final int snapshotChunkMaxSize;
  private final long halfMinTimeoutMs;

  private final AppenderDaemon daemon;

  public LogAppender(RaftServer.Division server, LeaderState leaderState, FollowerInfo f) {
    this.follower = f;
    this.name = follower.getName() + "-" + JavaUtils.getClassSimpleName(getClass());
    this.server = server;
    this.leaderState = leaderState;
    this.raftLog = server.getRaftLog();

    final RaftProperties properties = server.getRaftServer().getProperties();
    this.snapshotChunkMaxSize = RaftServerConfigKeys.Log.Appender.snapshotChunkSizeMax(properties).getSizeInt();
    this.halfMinTimeoutMs = server.properties().minRpcTimeoutMs() / 2;

    final SizeInBytes bufferByteLimit = RaftServerConfigKeys.Log.Appender.bufferByteLimit(properties);
    final int bufferElementLimit = RaftServerConfigKeys.Log.Appender.bufferElementLimit(properties);
    this.buffer = new DataQueue<>(this, bufferByteLimit, bufferElementLimit, EntryWithData::getSerializedSize);
    this.daemon = new AppenderDaemon();
  }

  protected RaftServer.Division getServer() {
    return server;
  }

  protected RaftServerRpc getServerRpc() {
    return server.getRaftServer().getServerRpc();
  }

  public RaftLog getRaftLog() {
    return raftLog;
  }

  @Override
  public String toString() {
    return name;
  }

  void startAppender() {
    daemon.start();
  }

  public boolean isAppenderRunning() {
    return daemon.isRunning();
  }

  public void stopAppender() {
    daemon.stop();
  }

  public FollowerInfo getFollower() {
    return follower;
  }

  protected RaftPeerId getFollowerId() {
    return getFollower().getPeer().getId();
  }

  public LeaderState getLeaderState() {
    return leaderState;
  }

  private TermIndex getPrevious(long nextIndex) {
    if (nextIndex == RaftLog.LEAST_VALID_LOG_INDEX) {
      return null;
    }

    final long previousIndex = nextIndex - 1;
    final TermIndex previous = raftLog.getTermIndex(previousIndex);
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

  protected AppendEntriesRequestProto createRequest(long callId,
      boolean heartbeat) throws RaftLogIOException {
    final TermIndex previous = getPrevious(follower.getNextIndex());
    final long snapshotIndex = follower.getSnapshotIndex();
    final long heartbeatRemainingMs = getHeartbeatRemainingTime();
    if (heartbeatRemainingMs <= 0L || heartbeat) {
      // heartbeat
      return leaderState.newAppendEntriesRequestProto(follower, Collections.emptyList(), previous, callId);
    }

    Preconditions.assertTrue(buffer.isEmpty(), () -> "buffer has " + buffer.getNumElements() + " elements.");

    final long leaderNext = raftLog.getNextIndex();
    final long followerNext = follower.getNextIndex();
    final long halfMs = heartbeatRemainingMs/2;
    for (long next = followerNext; leaderNext > next && getHeartbeatRemainingTime() - halfMs > 0; ) {
      if (!buffer.offer(raftLog.getEntryWithData(next++))) {
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
    while (isAppenderRunning()) { // keep retrying for IOException
      try {
        if (request == null || request.getEntriesCount() == 0) {
          request = createRequest(DEFAULT_CALLID, false);
        }

        if (request == null) {
          LOG.trace("{} no entries to send now, wait ...", this);
          return null;
        } else if (!isAppenderRunning()) {
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
      if (isAppenderRunning()) {
        server.properties().rpcSleepTime().sleep();
      }
    }
    return null;
  }

  protected InstallSnapshotRequestProto newInstallSnapshotNotificationRequest(TermIndex firstAvailableLogTermIndex) {
    Preconditions.assertTrue(firstAvailableLogTermIndex.getIndex() > 0);
    synchronized (server) {
      return ServerProtoUtils.toInstallSnapshotRequestProto(server.getMemberId(), getFollowerId(),
          server.getInfo().getCurrentTerm(), firstAvailableLogTermIndex, server.getRaftConf());
    }
  }

  protected Iterable<InstallSnapshotRequestProto> newInstallSnapshotRequests(
      String requestId, SnapshotInfo snapshot) {
    return new InstallSnapshotRequests(server, getFollowerId(), requestId, snapshot, snapshotChunkMaxSize);
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

  protected SnapshotInfo shouldInstallSnapshot() {
    final long logStartIndex = raftLog.getStartIndex();
    // we should install snapshot if the follower needs to catch up and:
    // 1. there is no local log entry but there is snapshot
    // 2. or the follower's next index is smaller than the log start index
    if (follower.getNextIndex() < raftLog.getNextIndex()) {
      final SnapshotInfo snapshot = server.getStateMachine().getLatestSnapshot();
      if (follower.getNextIndex() < logStartIndex ||
          (logStartIndex == RaftLog.INVALID_LOG_INDEX && snapshot != null)) {
        return snapshot;
      }
    }
    return null;
  }

  /** Check and send appendEntries RPC */
  protected void runAppenderImpl() throws InterruptedException, IOException {
    while (isAppenderRunning()) {
      if (shouldSendRequest()) {
        SnapshotInfo snapshot = shouldInstallSnapshot();
        if (snapshot != null) {
          LOG.info("{}: followerNextIndex = {} but logStartIndex = {}, send snapshot {} to follower",
              this, follower.getNextIndex(), raftLog.getStartIndex(), snapshot);

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
      if (isAppenderRunning() && !shouldAppendEntries(follower.getNextIndex())) {
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

  public synchronized void notifyAppend() {
    this.notify();
  }

  /** Should the leader send appendEntries RPC to this follower? */
  protected boolean shouldSendRequest() {
    return shouldAppendEntries(follower.getNextIndex()) || heartbeatTimeout();
  }

  protected boolean haveLogEntriesToSendOut() {
    return shouldAppendEntries(follower.getNextIndex());
  }

  protected boolean isFollowerCommitBehindLastCommitIndex() {
    return raftLog.getLastCommittedIndex() > follower.getCommitIndex();
  }

  private boolean shouldAppendEntries(long followerIndex) {
    return followerIndex < raftLog.getNextIndex();
  }

  protected boolean heartbeatTimeout() {
    return getHeartbeatRemainingTime() <= 0;
  }

  /**
   * @return the time in milliseconds that the leader should send a heartbeat.
   */
  protected long getHeartbeatRemainingTime() {
    return halfMinTimeoutMs - follower.getLastRpcTime().elapsedTimeMs();
  }

  protected boolean checkResponseTerm(long responseTerm) {
    synchronized (server) {
      return isAppenderRunning() && leaderState.onFollowerTerm(follower, responseTerm);
    }
  }
}
