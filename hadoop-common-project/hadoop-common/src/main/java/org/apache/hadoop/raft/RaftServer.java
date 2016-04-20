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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
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

  private static class EntryQueue {
    private final Queue<RaftLog.Entry> queue = new LinkedList<>();

    synchronized int size() {
      return queue.size();
    }

    synchronized void addAll(RaftLog.Entry... entries) {
      queue.addAll(Arrays.asList(entries));
    }

    synchronized RaftLog.Entry[] removeAll() {
      final RaftLog.Entry[] entries = new RaftLog.Entry[size()];
      for(int i = 0; i < entries.length; i++) {
        entries[i] = queue.remove();
      }
      return entries;
    }
  }

  class Leader extends Role {
    private RaftLog.TermIndex previous;

    private final List<FollowerInfo> followers = new ArrayList<>(ensemable.size());

    Leader() {
      // set an expired rpc time so that heartbeats are sent to followers immediately.
      final long nextIndex = raftlog.getNextIndex();
      final long t = Time.monotonicNow() - RaftConstants.RPC_TIMEOUT_MAX_MS;
      for(RaftServer s : ensemable.getOtherServers()) {
        followers.add(new FollowerInfo(s, t, nextIndex));
      }
    }

    void init() {
      for(FollowerInfo f : followers) {
        f.rpcSender.start();
      }
    }

    /** Request received from client */
    void clientRequests(RaftLog.Message... messages) {
      for(FollowerInfo f : followers) {
        // TODO
      }
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
      private final EntryQueue entryQueue = new EntryQueue();

      private final Daemon rpcSender = new Daemon(new RpcSender());

      FollowerInfo(RaftServer server, long lastRpcTime, long nextIndex) {
        this.server = server;
        this.lastRpcTime = new AtomicLong(lastRpcTime);
      }

      void clientRequests(RaftLog.Entry... entries) {
        entryQueue.addAll(entries);
        notify();
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
        if (entryQueue.size() > 0) {
          return true;
        }
        return getHeartbeatRemainingTime() <= 0;
      }

      /** Send an appendEntries RPC; retry indefinitely. */
      private Response sendAppendEntriesWithRetries()
          throws InterruptedException, InterruptedIOException {
        RaftLog.Entry[] entries = {};
        for(int retry = 0;; retry++) {
          try {
            if (entries.length == 0) {
              entries = entryQueue.removeAll();
            }
            return server.appendEntries(id, state.getTerm(),
                previous, raftlog.getLastCommitted().getIndex(), entries);
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
            processResponse(r);
          }

          wait(getHeartbeatRemainingTime());
        }
      }

      void processResponse(Response r) {
        //TODO

      }

      class RpcSender implements Runnable {
        @Override
        public String toString() {
          return getClass().getSimpleName();
        }

        @Override
        public void run() {
          try {
            checkAndSendAppendEntries();
          } catch (InterruptedException | InterruptedIOException e) {
            LOG.info(getClass().getSimpleName() + " interrupted.", e);
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
    RaftLog.Entry.assertEntries(term, entries);

    final long currentTerm = state.getTerm();
    if (term > currentTerm) {
      //
    }
    if (term < currentTerm || !raftlog.contains(previous)) {
      return new Response(id, currentTerm, false);
    }
    if (entries.length > 0) {
      raftlog.check(entries[0]);
    }
    return null;
  }
}
