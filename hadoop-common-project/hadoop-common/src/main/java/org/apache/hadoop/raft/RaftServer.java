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
import java.util.Iterator;
import java.util.List;
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

    @Override
    void rpcTimeout() throws InterruptedException, IOException {
      // send heartbeats
      for(Iterator<RaftServer> i = servers.iterator(); i.hasNext(); ) {
        final RaftServer s = i.next();
        if (s != RaftServer.this) {
          Response r = s.appendEntries(id, state.getTerm(),
              null, commitIndex);
        }
      }
    }

    class FollowerInfo {
      private long lastRpcTime = Time.monotonicNow();
      private long nextIndex;
      private long matchIndex;
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
        state.setRole(new Leader()).rpcTimeout();
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
      for(final Iterator<RaftServer> i = servers.iterator(); i.hasNext(); ) {
        final RaftServer s = i.next();
        if (s != RaftServer.this) {
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
            if (granted > servers.size()/2) {
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

  private final List<RaftServer> servers = new ArrayList<>();
  private final int id;
  private final ServerState state = new ServerState();
  private RaftLog raftlog;


  private final ExecutorService executor = Executors.newCachedThreadPool();

  RaftServer(int id) {
    this.id = id;
  }

  @Override
  public Response requestVote(int candidateId, long term,
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
  public Response appendEntries(int leaderId, long term,
      RaftLog.TermIndex previous, long leaderCommit, RaftLog.Entry... entries)
          throws IOException {
    return null;
  }
}
