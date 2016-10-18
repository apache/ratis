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
package org.apache.raft.server;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Time;
import org.apache.raft.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.raft.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.raft.proto.RaftProtos.FileChunkProto;
import org.apache.raft.proto.RaftProtos.InstallSnapshotReplyProto;
import org.apache.raft.proto.RaftProtos.InstallSnapshotRequestProto;
import org.apache.raft.proto.RaftProtos.InstallSnapshotResult;
import org.apache.raft.proto.RaftProtos.LogEntryProto;
import org.apache.raft.server.LeaderState.StateUpdateEventType;
import org.apache.raft.server.protocol.ServerProtoUtils;
import org.apache.raft.server.protocol.TermIndex;
import org.apache.raft.server.storage.FileInfo;
import org.apache.raft.server.storage.RaftLog;
import org.apache.raft.statemachine.SnapshotInfo;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.apache.raft.server.RaftServerConfigKeys.RAFT_SERVER_LOG_APPENDER_BATCH_ENABLED_DEFAULT;
import static org.apache.raft.server.RaftServerConfigKeys.RAFT_SERVER_LOG_APPENDER_BATCH_ENABLED_KEY;
import static org.apache.raft.server.RaftServerConfigKeys.RAFT_SERVER_LOG_APPENDER_BUFFER_CAPACITY_DEFAULT;
import static org.apache.raft.server.RaftServerConfigKeys.RAFT_SERVER_LOG_APPENDER_BUFFER_CAPACITY_KEY;
import static org.apache.raft.server.RaftServerConstants.INVALID_LOG_INDEX;

/**
 * A daemon thread appending log entries to a follower peer.
 */
public class LogAppender extends Daemon {
  private static final Logger LOG = RaftServer.LOG;

  private final RaftServer server;
  private final LeaderState leaderState;
  private final RaftLog raftLog;
  private final FollowerInfo follower;
  private final int maxBufferEntryNum;
  private final boolean batchSending;
  private final LogEntryBuffer buffer;

  private volatile boolean sending = true;

  public LogAppender(RaftServer server, LeaderState leaderState, FollowerInfo f) {
    this.follower = f;
    this.server = server;
    this.leaderState = leaderState;
    this.raftLog = server.getState().getLog();
    this.maxBufferEntryNum = server.getProperties().getInt(
        RAFT_SERVER_LOG_APPENDER_BUFFER_CAPACITY_KEY,
        RAFT_SERVER_LOG_APPENDER_BUFFER_CAPACITY_DEFAULT);
    this.batchSending = server.getProperties().getBoolean(
        RAFT_SERVER_LOG_APPENDER_BATCH_ENABLED_KEY,
        RAFT_SERVER_LOG_APPENDER_BATCH_ENABLED_DEFAULT);
    this.buffer = new LogEntryBuffer();
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(" + server.getId() + " -> " +
        follower.getPeer().getId() + ")";
  }

  @Override
  public void run() {
    try {
      checkAndSendAppendEntries();
    } catch (InterruptedException | InterruptedIOException e) {
      LOG.info(this + " was interrupted: " + e);
    }
  }

  private boolean isSenderRunning() {
    return sending;
  }

  public void stopSender() {
    this.sending = false;
  }

  public FollowerInfo getFollower() {
    return follower;
  }

  /**
   * A buffer for log entries with size limitation.
   */
  private class LogEntryBuffer {
    private final List<LogEntryProto> buf = new ArrayList<>(maxBufferEntryNum);

    void addEntries(LogEntryProto... entries) {
      Collections.addAll(buf, entries);
    }

    boolean isFull() {
      return buf.size() >= maxBufferEntryNum;
    }

    boolean isEmpty() {
      return buf.isEmpty();
    }

    int getRemainingSlots() {
      return maxBufferEntryNum - buf.size();
    }

    AppendEntriesRequestProto getAppendRequest(TermIndex previous) {
      final AppendEntriesRequestProto request = server
          .createAppendEntriesRequest(follower.getPeer().getId(),
              previous, buf, !follower.isAttendingVote());
      buf.clear();
      return request;
    }

    int getPendingEntryNum() {
      return buf.size();
    }
  }

  private TermIndex getPrevious() {
    TermIndex previous = ServerProtoUtils.toTermIndex(
        raftLog.get(follower.getNextIndex() - 1));
    if (previous == null) {
      // if previous is null, nextIndex must be equal to the log start
      // index (otherwise we will install snapshot).
      Preconditions.checkState(follower.getNextIndex() == raftLog.getStartIndex(),
          "follower's next index %s, local log start index %s",
          follower.getNextIndex(), raftLog.getStartIndex());
      SnapshotInfo snapshot = server.getState().getLatestSnapshot();
      previous = snapshot == null ? null : snapshot.getTermIndex();
    }
    return previous;
  }

  private AppendEntriesRequestProto createRequest() {
    final TermIndex previous = getPrevious();
    final long leaderNext = raftLog.getNextIndex();
    final long next = follower.getNextIndex() + buffer.getPendingEntryNum();
    boolean toSend = false;
    if (leaderNext > next) {
      int num = Math.min(buffer.getRemainingSlots(), (int) (leaderNext - next));
      buffer.addEntries(raftLog.getEntries(next, next + num));
      if (buffer.isFull() || !batchSending) {
        // buffer is full or batch sending is disabled, send out a request
        toSend = true;
      }
    } else if (!buffer.isEmpty()) {
      // no new entries, then send out the entries in the buffer
      toSend = true;
    }
    if (toSend || shouldHeartbeat()) {
      return buffer.getAppendRequest(previous);
    }
    return null;
  }

  /** Send an appendEntries RPC; retry indefinitely. */
  private AppendEntriesReplyProto sendAppendEntriesWithRetries()
      throws InterruptedException, InterruptedIOException {
    int retry = 0;
    AppendEntriesRequestProto request = null;
    while (isSenderRunning()) { // keep retrying for IOException
      try {
        if (request == null || request.getEntriesCount() == 0) {
          request = createRequest();
        }
        if (request == null) {
          LOG.trace("{} need not send AppendEntries now." +
              " Wait for more entries.", server.getId());
          return null;
        }

        final AppendEntriesReplyProto r = server.getServerRpc()
            .sendAppendEntries(request);
        follower.lastRpcTime.set(Time.monotonicNow());

        return r;
      } catch (InterruptedIOException iioe) {
        throw iioe;
      } catch (IOException ioe) {
        LOG.debug(this + ": failed to send appendEntries; retry " + retry++, ioe);
      }
      if (isSenderRunning()) {
        Thread.sleep(leaderState.getSyncInterval());
      }
    }
    return null;
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

  // TODO inefficient using RPC. need to change to zero-copy transfer
  private InstallSnapshotReplyProto installSnapshot(SnapshotInfo snapshot)
      throws InterruptedException, InterruptedIOException {
    String requestId = UUID.randomUUID().toString();
    int requestIndex = 0;

    InstallSnapshotReplyProto reply = null;
    for (int i = 0; i < snapshot.getFiles().size(); i++) {
      FileInfo fileInfo = snapshot.getFiles().get(i);
      File snapshotFile = fileInfo.getPath().toFile();
      final long totalSize = snapshotFile.length();
      final int bufLength =
          (int) Math.min(leaderState.getSnapshotChunkMaxSize(), totalSize);
      final byte[] buf = new byte[bufLength];
      long offset = 0;
      int chunkIndex = 0;
      try (FileInputStream in = new FileInputStream(snapshotFile)) {

        while (offset < totalSize) {
          int targetLength = (int) Math.min(totalSize - offset,
              leaderState.getSnapshotChunkMaxSize());
          FileChunkProto chunk = readFileChunk(fileInfo, in, buf, targetLength,
              offset, chunkIndex);
          boolean done = (i == snapshot.getFiles().size() - 1) && chunk.getDone();
          InstallSnapshotRequestProto request =
              server.createInstallSnapshotRequest(follower.getPeer().getId(),
                  requestId, requestIndex++, snapshot,
                  Lists.newArrayList(chunk), done);

          reply = server.getServerRpc().sendInstallSnapshot(request);
          follower.lastRpcTime.set(Time.monotonicNow());

          if (!reply.getServerReply().getSuccess()) {
            return reply;
          }

          offset += targetLength;
          chunkIndex++;
        }
      } catch (InterruptedIOException iioe) {
        throw iioe;
      } catch (IOException ioe) {
        LOG.warn(this + ": failed to install SnapshotInfo at offset " + offset,
            ioe);
        return null;
      }
    }
    if (reply != null) {
      follower.updateMatchIndex(snapshot.getTermIndex().getIndex());
      follower.updateNextIndex(snapshot.getTermIndex().getIndex() + 1);
      LOG.info("{}: install snapshot-{} successfully on follower {}",
          server.getId(), snapshot.getTermIndex().getIndex(), follower.getPeer());
    }
    return reply;
  }

  /** Check and send appendEntries RPC */
  private void checkAndSendAppendEntries()
      throws InterruptedException, InterruptedIOException {
    while (isSenderRunning()) {
      if (shouldSendRequest()) {
        final long logStartIndex = raftLog.getStartIndex();
        SnapshotInfo snapshot = null;
        // we should install snapshot if the follower needs to catch up and:
        // 1. there is no local log entry but there is snapshot
        // 2. or the follower's next index is smaller than the log start index
        boolean toInstallSnapshot = false;
        if (follower.getNextIndex() < raftLog.getNextIndex()) {
          snapshot = server.getState().getLatestSnapshot();
          toInstallSnapshot = (follower.getNextIndex() < logStartIndex) ||
              (logStartIndex == INVALID_LOG_INDEX && snapshot != null);
        }

        if (toInstallSnapshot) {
          LOG.info("{}: follower {}'s next index is {}," +
              " log's start index is {}, need to install snapshot",
              server.getId(), follower.getPeer(), follower.getNextIndex(),
              logStartIndex);
          Preconditions.checkState(snapshot != null);

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
      if (isSenderRunning() && !shouldAppendEntries(
          follower.getNextIndex() + buffer.getPendingEntryNum())) {
        final long waitTime = getHeartbeatRemainingTime(follower.lastRpcTime.get());
        if (waitTime > 0) {
          synchronized (this) {
            wait(waitTime);
          }
        }
      }
    }
  }

  private void handleReply(AppendEntriesReplyProto reply) {
    if (reply != null) {
      switch (reply.getResult()) {
        case SUCCESS:
          final long oldNextIndex = follower.getNextIndex();
          final long nextIndex = reply.getNextIndex();
          Preconditions.checkState(nextIndex >= oldNextIndex,
              "old next index: %s, next index in the reply: %s", oldNextIndex,
              nextIndex);

          if (nextIndex > oldNextIndex) {
            follower.updateMatchIndex(nextIndex - 1);
            follower.updateNextIndex(nextIndex);

            LeaderState.StateUpdateEvent e = follower.isAttendingVote() ?
                LeaderState.UPDATE_COMMIT_EVENT :
                LeaderState.STAGING_PROGRESS_EVENT;
            leaderState.submitUpdateStateEvent(e);
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

  public synchronized void notifyAppend() {
    this.notify();
  }

  /** Should the leader send appendEntries RPC to this follower? */
  private boolean shouldSendRequest() {
    return shouldAppendEntries(follower.getNextIndex()) || shouldHeartbeat();
  }

  private boolean shouldAppendEntries(long followerIndex) {
    return followerIndex < raftLog.getNextIndex();
  }

  private boolean shouldHeartbeat() {
    return getHeartbeatRemainingTime(follower.lastRpcTime.get()) <= 0;
  }

  /**
   * @return the time in milliseconds that the leader should send a heartbeat.
   */
  private long getHeartbeatRemainingTime(long lastTime) {
    return lastTime + server.minTimeout / 2 - Time.monotonicNow();
  }

  private void checkResponseTerm(long responseTerm) {
    synchronized (server) {
      if (isSenderRunning() && follower.isAttendingVote()
          && responseTerm > leaderState.getCurrentTerm()) {
        leaderState.submitUpdateStateEvent(
            new LeaderState.StateUpdateEvent(StateUpdateEventType.STEPDOWN,
                responseTerm));
      }
    }
  }
}
