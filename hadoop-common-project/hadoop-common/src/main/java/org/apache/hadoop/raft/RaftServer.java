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
package org.apache.hadoop.raft;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Time;

import com.google.common.base.Preconditions;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class RaftServer implements RaftProtocol {
  static final Log LOG = LogFactory.getLog(RaftServer.class);

  static abstract class Role {
    /** Handle rpc timeout. */
    void idleRpcTimeout() throws InterruptedException, IOException {
    }
  }

  class Leader extends Role {
    private final List<FollowerInfo> followers = new ArrayList<>(ensemable.size());

    Leader() {
      // set an expired rpc time so that heartbeats are sent to followers immediately.
      final long nextIndex = raftlog.getNextIndex();
      final long t = Time.monotonicNow() - RaftConstants.RPC_TIMEOUT_MAX_MS;
      for(RaftServer s : ensemable.getOtherServers()) {
        followers.add(new FollowerInfo(s, t, nextIndex));
      }
    }

    /** Request received from client */
    void clientRequests(RaftLog.Message message) {
      raftlog.apply(state.getCurrentTerm(), message);

      for(FollowerInfo f : followers) {
        f.notify();
      }
    }

    void startRpcSenders() {
      for(FollowerInfo f : followers) {
        f.rpcSender.start();
      }
    }

    void interruptRpcSenders() {
      for(FollowerInfo f : followers) {
        f.rpcSender.interrupt();
      }
    }

    void checkResponseTerm(long reponseTerm) {
      if (reponseTerm > state.getCurrentTerm()) {
        changeToFollower();
      }
    }

    void updateLastCommitted() {
      final long[] indices = new long[followers.size() + 1];
      for(int i = 0; i < followers.size(); i++) {
        indices[i] = followers.get(i).matchIndex.get();
      }
      indices[followers.size()] = raftlog.getNextIndex() - 1;

      Arrays.sort(indices);
      raftlog.setLastCommitted(indices[(indices.length - 1)/2]);
    }

    private class FollowerInfo {
      private final RaftServer server;
      private final AtomicLong lastRpcTime;

      private long nextIndex;
      private final AtomicLong matchIndex = new AtomicLong();

      private final Daemon rpcSender = new Daemon(new RpcSender());

      FollowerInfo(RaftServer server, long lastRpcTime, long nextIndex) {
        this.server = server;
        this.lastRpcTime = new AtomicLong(lastRpcTime);
        this.nextIndex = nextIndex;
      }

      void updateMatchIndex(final long matchIndex) {
        this.matchIndex.set(matchIndex);
        updateLastCommitted();
      }

      /**
       * @return the time in milliseconds that the leader should send a
       *         heartbeat this follower.
       */
      private long getHeartbeatRemainingTime() {
        return lastRpcTime.get() + RaftConstants.RPC_TIMEOUT_MIN_MS/2
            - Time.monotonicNow();
      }

      /** Should the leader send appendEntries RPC to this follower? */
      private boolean shouldSend() {
        return raftlog.get(nextIndex) != null
            || getHeartbeatRemainingTime() <= 0;
      }

      /** Send an appendEntries RPC; retry indefinitely. */
      private Response sendAppendEntriesWithRetries()
          throws InterruptedException, InterruptedIOException {
        RaftLog.Entry[] entries = {};
        for(int retry = 0;; retry++) {
          try {
            if (entries.length == 0) {
              entries = raftlog.getEntries(nextIndex);
            }
            final RaftLog.TermIndex previous = raftlog.get(nextIndex - 1);
            final Response r =  server.appendEntries(id, state.getCurrentTerm(),
                previous, raftlog.getLastCommitted().getIndex(), entries);
            if (r.success) {
              final long mi = entries[entries.length - 1].getIndex();
              updateMatchIndex(mi);;
              nextIndex = mi + 1;
            }
            return r;
          } catch (InterruptedIOException iioe) {
            throw iioe;
          } catch (IOException ioe) {
            LOG.warn("Failed to send appendEntries to " + server
                + "; retry " + retry, ioe);
          }

          Thread.sleep(RaftConstants.RPC_SLEEP_TIME_MS);
        }
      }

      /** Check and send appendEntries RPC */
      private void checkAndSendAppendEntries()
          throws InterruptedException, InterruptedIOException {
        for(;;) {
          if (shouldSend()) {
            final Response r = sendAppendEntriesWithRetries();
            lastRpcTime.set(Time.monotonicNow());

            checkResponseTerm(r.term);
            if (!r.success) {
              nextIndex--; // may implements the optimization in Section 5.3
            }
          }

          wait(getHeartbeatRemainingTime());
        }
      }

      class RpcSender implements Runnable {
        @Override
        public String toString() {
          return getClass().getSimpleName() + server.id;
        }

        @Override
        public void run() {
          try {
            checkAndSendAppendEntries();
          } catch (InterruptedException | InterruptedIOException e) {
            LOG.info(this + " is interrupted.", e);
          }
        }
      }
    }
  }

  class Follower extends Role {
    @Override
    void idleRpcTimeout() throws InterruptedException, IOException {
      changeRole(new Candidate()).idleRpcTimeout();
    }
  }

  class Candidate extends Role {
    @Override
    void idleRpcTimeout() throws InterruptedException, IOException {
      for(;;) {
        final long electionTerm = state.initElection(id);
        final LeaderElection.Result r = new LeaderElection(
            RaftServer.this, electionTerm).begin();

        synchronized(state) {
          if (electionTerm != state.getCurrentTerm() || !state.isCandidate()) {
            return; // term already passed or no longer a candidate.
          }

          switch(r) {
          case ELECTED:
            changeToLeader();
            return;
          case REJECTED:
          case NEWTERM:
            changeToFollower();
            return;
          case TIMEOUT:
            // should start another election
          }
        }
      }
    }
  }

  class RpcMonitor implements Runnable {
    private final AtomicLong lastRpcTime = new AtomicLong(Time.monotonicNow());

    void updateLastRpcTime(long now) {
      lastRpcTime.set(now);
    }

    @Override
    public  void run() {
      for(;;) {
        final long waitTime = RaftConstants.getRandomElectionWaitTime();
        try {
          if (waitTime > 0 ) {
            synchronized(this) {
              wait(waitTime);
            }
          }
          final long now = Time.monotonicNow();
          if (now >= lastRpcTime.get() + waitTime) {
            updateLastRpcTime(now);
            state.getRole().idleRpcTimeout();
          }
        } catch (InterruptedException e) {
          LOG.info(getClass().getSimpleName() + " interrupted.");
          return;
        } catch (Exception e) {
          LOG.warn(getClass().getSimpleName(), e);
        }
      }
    }
  }

  class Ensemable {
    private final Map<String, RaftServer> otherServers = new HashMap<>();

    Collection<RaftServer> getOtherServers() {
      return otherServers.values();
    }

    int size() {
      return otherServers.size() + 1;
    }
  }

  private final String id;
  private final ServerState state = new ServerState();

  private RaftLog raftlog;

  private final Ensemable ensemable = new Ensemable();

  private final RpcMonitor rpcMonitor = new RpcMonitor();
  private final Daemon rpcMonitorDaemon = new Daemon(rpcMonitor);

  RaftServer(String id) {
    this.id = id;
  }

  Ensemable getEnsemable() {
    return ensemable;
  }

  Response sendRequestVote(long term, RaftServer s) throws RaftServerException {
    for(;;) {
      try {
        return s.requestVote(id, term, raftlog.getLastCommitted());
      } catch (IOException e) {
        throw new RaftServerException(s.id, e);
      }
    }
  }

  private <R extends Role> R changeRole(R newRole) {
    state.changeRole(newRole);
    return newRole;
  }

  void changeToFollower() {
    if (state.isLeader()) {
      ((Leader)state.getRole()).interruptRpcSenders();
    }
    if (!state.isFollower()) {
      changeRole(new Follower());
    }
  }

  void changeToLeader() {
    Preconditions.checkState(state.isCandidate());
    changeRole(new Leader()).startRpcSenders();
  }

  @Override
  public Response requestVote(String candidateId, long candidateTerm,
      RaftLog.TermIndex lastCommitted) throws IOException {
    final long startTime = Time.monotonicNow();
    boolean voteGranted = false;
    synchronized (state) {
      if (state.recognizeCandidate(candidateId, candidateTerm)) {
        changeToFollower();
        rpcMonitor.updateLastRpcTime(startTime);

        // see Section 5.4.1 Election restriction
        if (raftlog.getLastCommitted().compareTo(lastCommitted) <= 0) {
          voteGranted = state.vote(candidateId, candidateTerm);
        }
      }
      return new Response(candidateId, state.getCurrentTerm(), voteGranted);
    }
  }

  @Override
  public Response appendEntries(String leaderId, long leaderTerm,
      RaftLog.TermIndex previous, long leaderCommit, RaftLog.Entry... entries)
          throws IOException {
    final long startTime = Time.monotonicNow();
    RaftLog.Entry.assertEntries(leaderTerm, entries);

    final long currentTerm;
    synchronized (state) {
      final boolean recognized = state.recognizeLeader(leaderId, leaderTerm);
      currentTerm = state.getCurrentTerm();
      if (!recognized) {
        return new Response(id, currentTerm, false);
      }
      changeToFollower();

      Preconditions.checkState(currentTerm == leaderTerm);
      rpcMonitor.updateLastRpcTime(startTime);

      if (previous != null && !raftlog.contains(previous)) {
        return new Response(id, currentTerm, false);
      }
      raftlog.apply(entries);
      return new Response(id, currentTerm, true);
    }
  }
}