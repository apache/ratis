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
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftLog.EntryWithData;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.server.storage.RaftLog;
import org.apache.ratis.server.storage.RaftLogIOException;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.file.Path;
import java.util.*;

import static org.apache.ratis.server.impl.RaftServerConstants.DEFAULT_CALLID;
import static org.apache.ratis.server.impl.RaftServerConstants.INVALID_LOG_INDEX;
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

  protected final RaftServerImpl server;
  private final LeaderState leaderState;
  protected final RaftLog raftLog;
  protected final FollowerInfo follower;

  private final DataQueue<EntryWithData> buffer;
  private final int snapshotChunkMaxSize;
  protected final long halfMinTimeoutMs;

  private final LifeCycle lifeCycle;
  private final Daemon daemon = new Daemon(this::runAppender);

  public LogAppender(RaftServerImpl server, LeaderState leaderState, FollowerInfo f) {
    this.follower = f;
    this.server = server;
    this.leaderState = leaderState;
    this.raftLog = server.getState().getLog();

    final RaftProperties properties = server.getProxy().getProperties();
    this.snapshotChunkMaxSize = RaftServerConfigKeys.Log.Appender.snapshotChunkSizeMax(properties).getSizeInt();
    this.halfMinTimeoutMs = server.getMinTimeoutMs() / 2;

    final SizeInBytes bufferByteLimit = RaftServerConfigKeys.Log.Appender.bufferByteLimit(properties);
    final int bufferElementLimit = RaftServerConfigKeys.Log.Appender.bufferElementLimit(properties);
    this.buffer = new DataQueue<>(this, bufferByteLimit, bufferElementLimit, EntryWithData::getSerializedSize);
    this.lifeCycle = new LifeCycle(this);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(" + server.getId() + " -> " +
        follower.getPeer().getId() + ")";
  }

  public void startAppender() {
    lifeCycle.transition(STARTING);
    daemon.start();
  }

  private void runAppender() {
    lifeCycle.transition(RUNNING);
    try {
      runAppenderImpl();
    } catch (InterruptedException | InterruptedIOException e) {
      LOG.info(this + " was interrupted: " + e);
    } catch (IOException e) {
      LOG.error(this + " hit IOException while loading raft log", e);
      lifeCycle.transition(EXCEPTION);
    } catch (Throwable e) {
      LOG.error(this + " unexpected exception", e);
      lifeCycle.transition(EXCEPTION);
    } finally {
      if (!lifeCycle.compareAndTransition(CLOSING, CLOSED)) {
        lifeCycle.transitionIfNotEqual(EXCEPTION);
      }
    }
  }

  protected boolean isAppenderRunning() {
    return !lifeCycle.getCurrentState().isOneOf(CLOSING, CLOSED, EXCEPTION);
  }

  public void stopAppender() {
    if (lifeCycle.compareAndTransition(NEW, CLOSED)) {
      return;
    }
    lifeCycle.transition(CLOSING);
    daemon.interrupt();
  }

  public FollowerInfo getFollower() {
    return follower;
  }

  RaftPeerId getFollowerId() {
    return getFollower().getPeer().getId();
  }

  private TermIndex getPrevious() {
    TermIndex previous = raftLog.getTermIndex(follower.getNextIndex() - 1);
    if (previous == null) {
      // if previous is null, nextIndex must be equal to the log start
      // index (otherwise we will install snapshot).
      Preconditions.assertTrue(follower.getNextIndex() == raftLog.getStartIndex(),
          "%s: follower's next index %s, local log start index %s",
          this, follower.getNextIndex(), raftLog.getStartIndex());
      SnapshotInfo snapshot = server.getState().getLatestSnapshot();
      previous = snapshot == null ? null : snapshot.getTermIndex();
    }
    return previous;
  }

  protected AppendEntriesRequestProto createRequest(long callId) throws RaftLogIOException {
    final TermIndex previous = getPrevious();
    final long heartbeatRemainingMs = getHeartbeatRemainingTime();
    if (heartbeatRemainingMs <= 0L) {
      return leaderState.newAppendEntriesRequestProto(
          getFollowerId(), previous, Collections.emptyList(), !follower.isAttendingVote(), callId);
    }

    Preconditions.assertTrue(buffer.isEmpty(), () -> "buffer has " + buffer.getNumElements() + " elements.");

    final long leaderNext = raftLog.getNextIndex();
    for (long next = follower.getNextIndex(); leaderNext > next; ) {
      if (!buffer.offer(raftLog.getEntryWithData(next++))) {
        break;
      }
    }
    if (buffer.isEmpty()) {
      return null;
    }

    final List<LogEntryProto> protos = buffer.pollList(heartbeatRemainingMs, EntryWithData::getEntry,
        (entry, time, exception) -> LOG.warn(this + ": Failed get " + entry + " in " + time, exception));
    buffer.clear();
    return leaderState.newAppendEntriesRequestProto(
        getFollowerId(), previous, protos, !follower.isAttendingVote(), callId);
  }

  /** Send an appendEntries RPC; retry indefinitely. */
  private AppendEntriesReplyProto sendAppendEntriesWithRetries()
      throws InterruptedException, InterruptedIOException, RaftLogIOException {
    int retry = 0;
    AppendEntriesRequestProto request = null;
    while (isAppenderRunning()) { // keep retrying for IOException
      try {
        if (request == null || request.getEntriesCount() == 0) {
          request = createRequest(DEFAULT_CALLID);
        }

        if (request == null) {
          LOG.trace("{} need not send AppendEntries now." +
              " Wait for more entries.", server.getId());
          return null;
        } else if (!isAppenderRunning()) {
          LOG.debug("LogAppender {} has been stopped. Skip the request.", this);
          return null;
        }

        follower.updateLastRpcSendTime();
        final AppendEntriesReplyProto r = server.getServerRpc().appendEntries(request);
        follower.updateLastRpcResponseTime();

        updateCommitIndex(r.getFollowerCommit());
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
        leaderState.getSyncInterval().sleep();
      }
    }
    return null;
  }

  protected void updateCommitIndex(long commitIndex) {
    if (follower.updateCommitIndex(commitIndex)) {
      leaderState.commitIndexChanged();
    }
  }

  protected class SnapshotRequestIter
      implements Iterable<InstallSnapshotRequestProto> {
    private final SnapshotInfo snapshot;
    private final List<FileInfo> files;
    private FileInputStream in;
    private int fileIndex = 0;

    private FileInfo currentFileInfo;
    private byte[] currentBuf;
    private long currentFileSize;
    private long currentOffset = 0;
    private int chunkIndex = 0;

    private final String requestId;
    private int requestIndex = 0;

    public SnapshotRequestIter(SnapshotInfo snapshot, String requestId)
        throws IOException {
      this.snapshot = snapshot;
      this.requestId = requestId;
      this.files = snapshot.getFiles();
      if (files.size() > 0) {
        startReadFile();
      }
    }

    private void startReadFile() throws IOException {
      currentFileInfo = files.get(fileIndex);
      File snapshotFile = currentFileInfo.getPath().toFile();
      currentFileSize = snapshotFile.length();
      final int bufLength = getSnapshotChunkLength(currentFileSize);
      currentBuf = new byte[bufLength];
      currentOffset = 0;
      chunkIndex = 0;
      in = new FileInputStream(snapshotFile);
    }

    private int getSnapshotChunkLength(long len) {
      return len < snapshotChunkMaxSize? (int)len: snapshotChunkMaxSize;
    }

    @Override
    public Iterator<InstallSnapshotRequestProto> iterator() {
      return new Iterator<InstallSnapshotRequestProto>() {
        @Override
        public boolean hasNext() {
          return fileIndex < files.size();
        }

        @Override
        public InstallSnapshotRequestProto next() {
          if (fileIndex >= files.size()) {
            throw new NoSuchElementException();
          }
          final int targetLength = getSnapshotChunkLength(
              currentFileSize - currentOffset);
          FileChunkProto chunk;
          try {
            chunk = readFileChunk(currentFileInfo, in, currentBuf,
                targetLength, currentOffset, chunkIndex);
            boolean done = (fileIndex == files.size() - 1) &&
                chunk.getDone();
            InstallSnapshotRequestProto request =
                server.createInstallSnapshotRequest(follower.getPeer().getId(),
                    requestId, requestIndex++, snapshot,
                    Collections.singletonList(chunk), done);
            currentOffset += targetLength;
            chunkIndex++;

            if (currentOffset >= currentFileSize) {
              in.close();
              fileIndex++;
              if (fileIndex < files.size()) {
                startReadFile();
              }
            }

            return request;
          } catch (IOException e) {
            if (in != null) {
              try {
                in.close();
              } catch (IOException ignored) {
              }
            }
            LOG.warn("Got exception when preparing InstallSnapshot request", e);
            throw new RuntimeException(e);
          }
        }
      };
    }
  }

  private FileChunkProto readFileChunk(FileInfo fileInfo,
      FileInputStream in, byte[] buf, int length, long offset, int chunkIndex)
      throws IOException {
    FileChunkProto.Builder builder = FileChunkProto.newBuilder()
        .setOffset(offset).setChunkIndex(chunkIndex);
    IOUtils.readFully(in, buf, 0, length);
    Path relativePath = server.getState().getStorage().getStorageDir()
        .relativizeToRoot(fileInfo.getPath());
    builder.setFilename(relativePath.toString());
    builder.setDone(offset + length == fileInfo.getFileSize());
    builder.setFileDigest(
        ByteString.copyFrom(fileInfo.getFileDigest().getDigest()));
    builder.setData(ByteString.copyFrom(buf, 0, length));
    return builder.build();
  }

  private InstallSnapshotReplyProto installSnapshot(SnapshotInfo snapshot) throws InterruptedIOException {
    String requestId = UUID.randomUUID().toString();
    InstallSnapshotReplyProto reply = null;
    try {
      for (InstallSnapshotRequestProto request :
          new SnapshotRequestIter(snapshot, requestId)) {
        follower.updateLastRpcSendTime();
        reply = server.getServerRpc().installSnapshot(request);
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
      LOG.info("{}: install snapshot-{} successfully on follower {}",
          server.getId(), snapshot.getTermIndex().getIndex(), follower.getPeer());
    }
    return reply;
  }

  protected SnapshotInfo shouldInstallSnapshot() {
    final long logStartIndex = raftLog.getStartIndex();
    // we should install snapshot if the follower needs to catch up and:
    // 1. there is no local log entry but there is snapshot
    // 2. or the follower's next index is smaller than the log start index
    if (follower.getNextIndex() < raftLog.getNextIndex()) {
      SnapshotInfo snapshot = server.getState().getLatestSnapshot();
      if (follower.getNextIndex() < logStartIndex ||
          (logStartIndex == INVALID_LOG_INDEX && snapshot != null)) {
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
          LOG.info("{}: follower {}'s next index is {}," +
              " log's start index is {}, need to install snapshot",
              server.getId(), follower.getPeer(), follower.getNextIndex(),
              raftLog.getStartIndex());

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
      checkSlowness();
    }
  }

  private void handleReply(AppendEntriesReplyProto reply) {
    if (reply != null) {
      switch (reply.getResult()) {
        case SUCCESS:
          final long oldNextIndex = follower.getNextIndex();
          final long nextIndex = reply.getNextIndex();
          if (nextIndex < oldNextIndex) {
            throw new IllegalStateException("nextIndex=" + nextIndex
                + " < oldNextIndex=" + oldNextIndex
                + ", reply=" + ProtoUtils.toString(reply));
          }

          if (nextIndex > oldNextIndex) {
            follower.updateMatchIndex(nextIndex - 1);
            follower.updateNextIndex(nextIndex);
            submitEventOnSuccessAppend();
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
          LOG.warn("{} received UNRECOGNIZED AppendResult from {}",
              server.getId(), follower.getPeer().getId());
          break;
      }
    }
  }

  private void handleException(Exception e) {
    LOG.trace("TRACE", e);
    server.getServerRpc().handleException(follower.getPeer().getId(), e, false);
  }

  protected void submitEventOnSuccessAppend() {
    if (follower.isAttendingVote()) {
      leaderState.submitUpdateCommitEvent();
    } else {
      leaderState.submitCheckStagingEvent();
    }
  }

  protected void checkSlowness() {
    if (follower.isSlow()) {
      server.getStateMachine().notifySlowness(server.getGroup(), server.getRoleInfoProto());
    }
  }

  public synchronized void notifyAppend() {
    this.notify();
  }

  /** Should the leader send appendEntries RPC to this follower? */
  protected boolean shouldSendRequest() {
    return shouldAppendEntries(follower.getNextIndex()) || shouldHeartbeat();
  }

  private boolean shouldAppendEntries(long followerIndex) {
    return followerIndex < raftLog.getNextIndex();
  }

  private boolean shouldHeartbeat() {
    return getHeartbeatRemainingTime() <= 0;
  }

  /**
   * @return the time in milliseconds that the leader should send a heartbeat.
   */
  protected long getHeartbeatRemainingTime() {
    return halfMinTimeoutMs - follower.getLastRpcTime().elapsedTimeMs();
  }

  protected void checkResponseTerm(long responseTerm) {
    synchronized (server) {
      if (isAppenderRunning() && follower.isAttendingVote()
          && responseTerm > leaderState.getCurrentTerm()) {
        leaderState.submitStepDownEvent(responseTerm);
      }
    }
  }
}
