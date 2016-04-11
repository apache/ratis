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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Time;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class RaftServer implements RaftProtocol {
  public static final Log LOG = LogFactory.getLog(RaftServer.class);

  static abstract class Role {
    abstract void rpcTimeout() throws InterruptedException, IOException;
  }

  class Leader extends Role {
    private long commitIndex = -1;

    private final List<FollowerInfo> followers = new ArrayList<>(ensemable.size());
    private final Daemon heartbeater = new Daemon(new Heartbeater());

    Leader() {
      // set an expired rpc time so that heartbeats are sent to followers immediately.
      final long t = Time.monotonicNow() - RaftConstants.RPC_TIMEOUT_MAX_MS;
      for(RaftServer s : ensemable.getOtherServers()) {
        followers.add(new FollowerInfo(s, t));
      }
    }

    void init() {
      heartbeater.start();
    }

    /** Request received from client */
    void clientRequests(RaftLog.Entry... entries) {

    }

    @Override
    void rpcTimeout() throws InterruptedException, IOException {
      throw new IllegalStateException(getClass().getSimpleName()
          + " should not have RPC timeout.");
    }

    private class FollowerInfo {
      private final RaftServer server;
      private final AtomicLong lastRpcTime;

      private long nextIndex;
      private long matchIndex;

      FollowerInfo(RaftServer server, long lastRpcTime) {
        this.server = server;
        this.lastRpcTime = new AtomicLong(lastRpcTime);
      }

      boolean shouldSendHeartbeat() {
        final long now = Time.monotonicNow();
        return now > lastRpcTime.get() + RaftConstants.RPC_TIMEOUT_MIN_MS/2;
      }

      void sendHeartbeat() throws InterruptedException {
        for(int i = 0;; i++) {
          if (!shouldSendHeartbeat()) {
            return;
          }

          try {
            server.appendEntries(id, state.getTerm(),
                null, commitIndex);
            lastRpcTime.set(Time.monotonicNow());
            return;
          } catch (IOException e) {
            LOG.warn("Failed to send heartbeat to " + server + " " + i, e);
          }

          Thread.sleep(RaftConstants.RPC_SLEEP_TIME_MS);
        }
      }

      void sendRequest(RaftLog.Entry... entries) throws InterruptedException {
        for(int i = 0;; i++) {
          if (!shouldSendHeartbeat()) {
            return;
          }

          try {
            server.appendEntries(id, state.getTerm(),
                null, commitIndex);
            lastRpcTime.set(Time.monotonicNow());
            return;
          } catch (IOException e) {
            LOG.warn("Failed to send heartbeat to " + server + " " + i, e);
          }

          Thread.sleep(RaftConstants.RPC_SLEEP_TIME_MS);
        }
      }
    }

    class Heartbeater implements Runnable {
      @Override
      public String toString() {
        return getClass().getSimpleName();
      }

      @Override
      public void run() {
        for(;;) {
          checkTimeout();

          try {
            Thread.sleep(RaftConstants.RPC_SLEEP_TIME_MS);
          } catch (InterruptedException e) {
            LOG.info(getClass().getSimpleName() + " interrupted.", e);
            return;
          }
        }
      }

      private void checkTimeout() {
        for(final FollowerInfo f : followers) {
          if (f.shouldSendHeartbeat()) {
            executor.submit(new Callable<Void>() {
              @Override
              public Void call() throws InterruptedException {
                f.sendHeartbeat();
                return null;
              }
            });
          }
        }
      }
    }
  }

  class Follower extends Role {
    @Override
    void rpcTimeout() throws InterruptedException, IOException {
      state.setRole(new Candidate()).rpcTimeout();
    }
  }

  class Candidate extends Role {
    @Override
    void rpcTimeout() throws InterruptedException, IOException {
      final long newTerm = state.initElection(id);
      if (beginElection(newTerm)) {
        state.setRole(new Leader()).init();
      }
    }

    /** Begin an election and try to become a leader. */
    private boolean beginElection(final long newTerm)
        throws InterruptedException, RaftException {
      final long startTime = Time.monotonicNow();
      final long timeout = startTime + RaftConstants.getRandomElectionWaitTime();
      final ExecutorCompletionService<Response> completion
          = new ExecutorCompletionService<>(executor);

      // submit requestVote to all servers
      int submitted = 0;
      for(final RaftServer s : ensemable.getOtherServers()) {
        submitted++;
        completion.submit(new Callable<Response>() {
          @Override
          public Response call() throws RaftServerException {
            try {
              return s.requestVote(id, newTerm, raftlog.getLastCommitted());
            } catch (IOException e) {
              throw new RaftServerException(s.id, e);
            }
          }
        });
      }

      // wait for responses
      final List<Response> responses = new ArrayList<>();
      final List<Exception> exceptions = new ArrayList<>();
      int granted = 0;
      for(;;) {
        final long waitTime = timeout - Time.monotonicNow();
        if (waitTime <= 0) {
          LOG.info("Election timeout: " + string(responses, exceptions));
          return false;
        }
        try {
          final Response r = completion.poll(waitTime, TimeUnit.MILLISECONDS).get();
          responses.add(r);
          if (r.success) {
            granted++;
            if (granted > ensemable.size()/2) {
              LOG.info("Election passed: " + string(responses, exceptions));
              return true;
            }
          }
        } catch(ExecutionException e) {
          LOG.warn("", e);
          exceptions.add(e);
        }

        if (responses.size() + exceptions.size() == submitted) {
          // received all the responses
          LOG.info("Election denied: " + string(responses, exceptions));
          return false;
        }
      }
    }
  }

  class RpcMonitor extends Daemon {
    private final AtomicLong lastRpcTime = new AtomicLong(Time.monotonicNow());

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
            lastRpcTime.set(now);
            state.getRole().rpcTimeout();
          }
        } catch (IOException e) {
          // TODO; handle exception
          e.printStackTrace();
        } catch (InterruptedException e) {
          LOG.info(getClass().getSimpleName() + " interrupted.");
          return;
        }
      }
    }
  }

  static String string(List<Response> responses, List<Exception> exceptions) {
    return "received " + responses.size() + " response(s) and "
        + exceptions.size() + " exception(s); "
        + responses + "; " + exceptions;
  }

  class Ensemable {
    private final Map<String, RaftServer> otherServers = new HashMap<>();

    Iterable<RaftServer> getOtherServers() {
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

  private final ExecutorService executor = Executors.newCachedThreadPool();

  RaftServer(String id) {
    this.id = id;
  }


  @Override
  public Response requestVote(String candidateId, long term,
      RaftLog.TermIndex lastCommitted) throws IOException {
    boolean voteGranted = false;
    synchronized (state) {
      if (state.isFollower()) {
        if (raftlog.getLastCommitted().compareTo(lastCommitted) <= 0) {
          voteGranted = state.vote(candidateId, term);
        }
      }
      return new Response(candidateId, state.getTerm(), voteGranted);
    }
  }

  @Override
  public Response appendEntries(String leaderId, long term,
      RaftLog.TermIndex previous, long leaderCommit, RaftLog.Entry... entries)
          throws IOException {
    final long currentTerm = state.getTerm();
    if (term < currentTerm) {
      return new Response(id, currentTerm, false);
    }
    return null;
  }
}
