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
package org.apache.hadoop.raft.server;

import org.apache.hadoop.raft.server.protocol.AppendEntriesRequest;
import org.apache.hadoop.raft.server.protocol.Entry;
import org.apache.hadoop.raft.server.protocol.RaftPeer;
import org.apache.hadoop.raft.server.protocol.RaftServerReply;
import org.apache.hadoop.raft.server.protocol.TermIndex;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * States for leader only
 */
class LeaderState extends Daemon {
  private static final Logger LOG = RaftServer.LOG;

  /**
   * @return the time in milliseconds that the leader should send a heartbeat.
   */
  private static long getHeartbeatRemainingTime(long lastTime) {
    return lastTime + RaftConstants.RPC_TIMEOUT_MIN_MS / 2 - Time.monotonicNow();
  }

  enum StateUpdateEvent {
    STEPDOWN, UPDATECOMMIT
  }

  private final RaftServer server;
  private final RaftLog raftLog;
  private final long currentTerm;

  private final List<RpcSender> senders;
  private final BlockingQueue<StateUpdateEvent> eventQ;
  private volatile boolean running = true;

  LeaderState(RaftServer server) {
    this.server = server;

    final ServerState state = server.getState();
    this.raftLog = state.getLog();
    this.currentTerm = state.getCurrentTerm();
    eventQ = new LinkedBlockingQueue<>();

    Collection<RaftPeer> others = server.getRaftConf()
        .getOtherPeers(state.getSelfId());
    final long t = Time.monotonicNow() - RaftConstants.RPC_TIMEOUT_MAX_MS;
    final long nextIndex = raftLog.getNextIndex();
    senders = new ArrayList<>(others.size());
    for (RaftPeer p : others) {
      FollowerInfo f = new FollowerInfo(p, t, nextIndex);
      senders.add(new RpcSender(f));
    }

    startSenders();
  }

  private void startSenders() {
    for (RpcSender sender : senders) {
      sender.start();
    }
  }

  void stopRunning() {
    this.running = false;
    for (RpcSender sender : senders) {
      sender.interrupt();
    }
  }

  void notifySenders() {
    for (RpcSender sender : senders) {
      synchronized (sender) {
        sender.notify();
      }
    }
  }

  void submitUpdateStateEvent(StateUpdateEvent e) {
    eventQ.offer(e);
  }

  /**
   * The LeaderState thread itself takes the responsibility to update
   * the raft server's state, such as changing to follower, or updating the
   * committed index.
   */
  @Override
  public void run() {
    while (running) {
      try {
        StateUpdateEvent event = eventQ.take();
        if (running) {
          handleEvent(event);
        }
      } catch (InterruptedException e) {
        if (!running) {
          LOG.info("The LeaderState gets is stopped");
        } else {
          LOG.warn("The leader election thread of peer {} is interrupted. "
              + "Currently role: {}.", server.getState().getSelfId(),
              server.getRole());
          throw new RuntimeException(e);
        }
      }
    }
  }

  void handleEvent(StateUpdateEvent e) {
    if (e == StateUpdateEvent.STEPDOWN) {
      server.changeToFollower();
    } else if (e == StateUpdateEvent.UPDATECOMMIT) {
      updateLastCommitted();
    }
  }

  private void updateLastCommitted() {
    final String selfId = server.getState().getSelfId();
    synchronized (server) {
      final RaftConfiguration conf = server.getRaftConf();
      List<List<FollowerInfo>> followerLists = divideFollowers(conf);
      long majorityInNewConf = computeLastCommitted(followerLists.get(0),
          conf.containsInConf(selfId));
      if (!conf.inTransitionState()) {
        raftLog.updateLastCommitted(majorityInNewConf, currentTerm);
      } else {
        long majorityInOldConf = computeLastCommitted(followerLists.get(1),
            conf.containsInOldConf(selfId));
        raftLog.updateLastCommitted(
            Math.min(majorityInNewConf, majorityInOldConf), currentTerm);
      }
    }
  }

  private long computeLastCommitted(List<FollowerInfo> followers,
      boolean includeSelf) {
    final int length = includeSelf ? followers.size() + 1 : followers.size();
    final long[] indices = new long[length];
    for (int i = 0; i < followers.size(); i++) {
      indices[i] = followers.get(i).matchIndex.get();
    }
    if (includeSelf) {
      indices[length - 1] = raftLog.getNextIndex() - 1;
    }

    Arrays.sort(indices);
    return indices[(indices.length - 1) / 2];
  }

  private List<List<FollowerInfo>> divideFollowers(RaftConfiguration conf) {
    List<List<FollowerInfo>> lists = new ArrayList<>(2);
    List<FollowerInfo> listForNew = new ArrayList<>();
    for (RpcSender sender : senders) {
      if (conf.containsInConf(sender.follower.peer.getId())) {
        listForNew.add(sender.follower);
      }
    }
    lists.add(listForNew);
    if (conf.inTransitionState()) {
      List<FollowerInfo> listForOld = new ArrayList<>();
      for (RpcSender sender : senders) {
        if (conf.containsInOldConf(sender.follower.peer.getId())) {
          listForOld.add(sender.follower);
        }
      }
      lists.add(listForOld);
    }
    return lists;
  }

  private class FollowerInfo {
    private final RaftPeer peer;
    private final AtomicLong lastRpcTime;
    private long nextIndex;
    private final AtomicLong matchIndex = new AtomicLong();

    FollowerInfo(RaftPeer peer, long lastRpcTime, long nextIndex) {
      this.peer = peer;
      this.lastRpcTime = new AtomicLong(lastRpcTime);
      this.nextIndex = nextIndex;
    }

    void updateMatchIndex(final long matchIndex) {
      this.matchIndex.set(matchIndex);
    }

    void updateNextIndex(long i) {
      nextIndex = i;
    }

    void decreaseNextIndex() {
      nextIndex--;
    }
  }

  class RpcSender extends Daemon {
    private final FollowerInfo follower;

    public RpcSender(FollowerInfo f) {
      this.follower = f;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "(" + server.getState().getSelfId()
          + " -> " + follower.peer.getId() + ")";
    }

    @Override
    public void run() {
      try {
        checkAndSendAppendEntries();
      } catch (InterruptedException | InterruptedIOException e) {
        LOG.info(this + " was interrupted: " + e);
        LOG.trace("TRACE", e);
      }
    }

    /** Send an appendEntries RPC; retry indefinitely. */
    private RaftServerReply sendAppendEntriesWithRetries()
        throws InterruptedException, InterruptedIOException {
      Entry[] entries = null;
      int retry = 0;
      while (running) {
        try {
          if (entries == null) {
            entries = raftLog.getEntries(follower.nextIndex);
          }
          final TermIndex previous = raftLog.get(follower.nextIndex - 1);
          AppendEntriesRequest request = server.createAppendEntriesRequest(
              follower.peer.getId(), previous, entries);
          final RaftServerReply r = server.sendAppendEntries(request);
          if (r.isSuccess() && entries != null && entries.length > 0) {
            final long mi = entries[entries.length - 1].getIndex();
            follower.updateMatchIndex(mi);
            follower.updateNextIndex(mi + 1);
            submitUpdateStateEvent(StateUpdateEvent.UPDATECOMMIT);
          }
          return r;
        } catch (InterruptedIOException iioe) {
          throw iioe;
        } catch (IOException ioe) {
          LOG.warn(this + ": failed to send appendEntries; retry " + retry++,
              ioe);
        }
        if (running) {
          Thread.sleep(RaftConstants.RPC_SLEEP_TIME_MS);
        }
      }
      return null;
    }

    /** Check and send appendEntries RPC */
    private void checkAndSendAppendEntries() throws InterruptedException,
        InterruptedIOException {
      while (running) {
        if (shouldSend()) {
          final RaftServerReply r = sendAppendEntriesWithRetries();
          if (r == null) {
            break;
          }
          follower.lastRpcTime.set(Time.monotonicNow());

          // check if should step down
          checkResponseTerm(r.getTerm());

          if (!r.isSuccess()) {
            // may implements the optimization in Section 5.3
            follower.decreaseNextIndex();
          }
        }
        if (running) {
          synchronized (this) {
            wait(getHeartbeatRemainingTime(follower.lastRpcTime.get()));
          }
        }
      }
    }

    /** Should the leader send appendEntries RPC to this follower? */
    private boolean shouldSend() {
      return raftLog.get(follower.nextIndex) != null ||
          getHeartbeatRemainingTime(follower.lastRpcTime.get()) <= 0;
    }

    private void checkResponseTerm(long responseTerm) {
      synchronized (server) {
        if (running && responseTerm > currentTerm) {
          submitUpdateStateEvent(StateUpdateEvent.STEPDOWN);
        }
      }
    }
  }
}
