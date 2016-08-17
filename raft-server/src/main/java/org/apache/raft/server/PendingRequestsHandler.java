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
import org.apache.hadoop.util.Daemon;
import org.apache.raft.protocol.RaftClientReply;
import org.apache.raft.protocol.RaftClientRequest;
import org.apache.raft.protocol.SetConfigurationRequest;
import org.slf4j.Logger;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.PriorityBlockingQueue;

class PendingRequestsHandler {
  private static final Logger LOG = RaftServer.LOG;

  private static class ConfigurationRequests {
    private final Queue<PendingRequest> results = new ConcurrentLinkedDeque<>();
    private volatile PendingRequest pendingRequest;

    synchronized void setPendingRequest(PendingRequest request) {
      Preconditions.checkState(pendingRequest == null);
      this.pendingRequest = request;
    }

    synchronized void finishSetConfiguration(boolean success) {
      // we allow the pendingRequest to be null in case that the new leader
      // commits the new configuration while it has not received the retry
      // request from the client
      if (pendingRequest != null) {
        pendingRequest.setReply(
            new RaftClientReply(pendingRequest.getRequest(), success, null));
        results.offer(pendingRequest);
        pendingRequest = null;
      }
    }

    void sendResults(RaftServerRpc rpc) {
      for(; !results.isEmpty();) {
        results.poll().sendReply(rpc);
      }
    }
  }

  private final RaftServer server;
  private final PriorityBlockingQueue<PendingRequest> queue =
      new PriorityBlockingQueue<>();
  private final ConfigurationRequests confRequests = new ConfigurationRequests();
  private final PendingRequestDaemon daemon = new PendingRequestDaemon();
  private volatile boolean running = true;

  public PendingRequestsHandler(RaftServer raftServer) {
    this.server = raftServer;
  }

  void start() {
    daemon.start();
  }

  void stop() {
    this.running = false;
    daemon.interrupt();
    try {
      daemon.join();
    } catch (InterruptedException e) {
      LOG.warn("{}'s pending request handler got interrupted", server.getId());
    }
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + "-" + server.getId();
  }

  PendingRequest addPendingRequest(long index, RaftClientRequest request) {
    final PendingRequest pending = new PendingRequest(index, request);
    final boolean b = queue.offer(pending);
    Preconditions.checkState(b);
    return pending;
  }

  PendingRequest addConfRequest(SetConfigurationRequest request) {
    final PendingRequest pending = new PendingRequest(request);
    confRequests.setPendingRequest(pending);
    return pending;
  }

  void finishSetConfiguration(boolean success) {
    confRequests.finishSetConfiguration(success);
  }

  synchronized void notifySendingDaemon() {
    this.notifyAll();
  }

  class PendingRequestDaemon extends Daemon {
    @Override
    public String toString() {
      return server.getId() + ": " + getClass().getSimpleName();
    }

    @Override
    public void run() {
      long lastCommitted = server.getState().getLog().getLastCommittedIndex();
      while (running) {
        try {
          final long oldLastCommitted = lastCommitted;
          for (PendingRequest pre;
               (pre = queue.peek()) != null && pre.getIndex() <= lastCommitted;
              ) {
            pre = queue.poll();
            sendSuccessReply(pre);
          }
          confRequests.sendResults(server.getServerRpc());
          lastCommitted = server.getState().getLog().getLastCommittedIndex();
          synchronized (PendingRequestsHandler.this) {
            if (oldLastCommitted == lastCommitted) {
              PendingRequestsHandler.this.wait(RaftServerConstants.RPC_TIMEOUT_MIN_MS);
            }
          }
        } catch (InterruptedException e) {
          LOG.info(this + " is interrupted.", e);
          break;
        } catch (Throwable t) {
          if (!running) {
            LOG.info(this + " is stopped.");
            break;
          }
          LOG.error(this + " is terminating due to", t);
        }
      }
      // The server has stepped down. Send a success response if the log
      // entry has been committed, or NotLeaderException
      sendResponses(server.getState().getLog().getLastCommittedIndex());
    }

    private void sendResponses(final long lastCommitted) {
      LOG.info(server.getId() +
          " sends responses before shutting down PendingRequestsHandler");
      while (!queue.isEmpty()) {
        final PendingRequest req = queue.poll();
        if (req.getIndex() <= lastCommitted) {
          sendSuccessReply(req);
        } else {
          sendNotLeaderException(req);
        }
      }
      confRequests.sendResults(server.getServerRpc());
      if (confRequests.pendingRequest != null) {
        sendNotLeaderException(confRequests.pendingRequest);
      }
    }

    private void sendReply(PendingRequest pending, RaftClientReply reply) {
      pending.setReply(reply);
      pending.sendReply(server.getServerRpc());
    }

    private void sendSuccessReply(PendingRequest pending) {
      sendReply(pending, new RaftClientReply(pending.getRequest(), true, null));
    }

    private void sendNotLeaderException(PendingRequest pending) {
      RaftClientReply reply = new RaftClientReply(pending.getRequest(), false,
          server.generateNotLeaderException());
      sendReply(pending, reply);
    }
  }
}
