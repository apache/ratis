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

import com.google.common.base.Preconditions;
import org.apache.hadoop.raft.protocol.RaftClientReply;
import org.apache.hadoop.raft.protocol.RaftClientRequest;
import org.apache.hadoop.raft.protocol.SetConfigurationRequest;
import org.apache.hadoop.util.Daemon;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;

class PendingRequestsHandler {
  private static final Logger LOG = RaftServer.LOG;

  private static class PendingRequestElement
      implements Comparable<PendingRequestElement> {
    private final long index;
    private final RaftClientRequest request;

    PendingRequestElement(long index, RaftClientRequest request) {
      this.index = index;
      this.request = request;
    }

    @Override
    public int compareTo(PendingRequestElement that) {
      return Long.compare(this.index, that.index);
    }
  }

  private static class ConfigurationRequests {
    private final Map<SetConfigurationRequest, Boolean> resultMap =
        new ConcurrentHashMap<>();
    private volatile SetConfigurationRequest pendingRequest;

    synchronized void setPendingRequest(SetConfigurationRequest request) {
      Preconditions.checkState(pendingRequest == null);
      this.pendingRequest = request;
    }

    synchronized void finishSetConfiguration(boolean success) {
      // we allow the pendingRequest to be null in case that the new leader
      // commits the new configuration while it has not received the retry
      // request from the client
      if (pendingRequest != null) {
        resultMap.put(pendingRequest, success);
        pendingRequest = null;
      }
    }
  }

  private final RaftServer server;
  private final PriorityBlockingQueue<PendingRequestElement> queue =
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

  void put(long index, RaftClientRequest request) {
    final boolean b = queue.offer(new PendingRequestElement(index, request));
    Preconditions.checkState(b);
  }

  void addConfRequest(SetConfigurationRequest request) {
    confRequests.setPendingRequest(request);
  }

  void finishSetConfiguration(boolean success) {
    confRequests.finishSetConfiguration(success);
  }

  private boolean sendReply(RaftClientRequest request, boolean success) {
    try {
      server.getServerRpc().sendClientReply(request,
          new RaftClientReply(request, success), null);
      return true;
    } catch (IOException ioe) {
      RaftServer.LOG.error(this + " has " + ioe);
      RaftServer.LOG.trace("TRACE", ioe);
      return false;
    }
  }

  private boolean sendNotLeaderException(RaftClientRequest request) {
    try {
      server.getServerRpc().sendClientReply(request, null,
          server.generateNotLeaderException());
      return true;
    } catch (IOException ioe) {
      RaftServer.LOG.error(this + " has " + ioe);
      RaftServer.LOG.trace("TRACE", ioe);
      return false;
    }
  }

  synchronized void notifySendingDaemon() {
    this.notifyAll();
  }

  class PendingRequestDaemon extends Daemon {
    @Override
    public String toString() {
      return getClass().getSimpleName();
    }

    @Override
    public void run() {
      long lastCommitted;
      while (running) {
        try {
          lastCommitted = server.getState().getLog().getLastCommittedIndex();
          for (PendingRequestElement pre;
               (pre = queue.peek()) != null && pre.index <= lastCommitted; ) {
            pre = queue.poll();
            sendReply(pre.request, true);
          }
          Iterator<Map.Entry<SetConfigurationRequest, Boolean>> iter =
              confRequests.resultMap.entrySet().iterator();
          while (iter.hasNext()) {
            Map.Entry<SetConfigurationRequest, Boolean> entry = iter.next();
            sendReply(entry.getKey(), entry.getValue());
            iter.remove();
          }
          synchronized (PendingRequestsHandler.this) {
            PendingRequestsHandler.this.wait(RaftConstants.RPC_TIMEOUT_MIN_MS);
          }
        } catch (InterruptedException e) {
          RaftServer.LOG.info(this + " is interrupted by ", e);
          break;
        } catch (Throwable t) {
          if (!running) {
            RaftServer.LOG.info(this + " is stopped.");
            break;
          }
          RaftServer.LOG.error(this + " is terminating due to", t);
        }
      }
      // The server has stepped down. Send a success response if the log
      // entry has been committed, or NotLeaderException
      sendResponses(server.getState().getLog().getLastCommittedIndex());
    }
  }

  private void sendResponses(long lastCommitted) {
    LOG.info(server.getId() +
        " sends responses before shutting down PendingRequestsHandler");
    while (!queue.isEmpty()) {
      final PendingRequestElement req = queue.poll();
      if (req.index <= lastCommitted) {
        sendReply(req.request, true);
      } else {
        sendNotLeaderException(req.request);
      }
    }
    for (Map.Entry<SetConfigurationRequest, Boolean> entry :
        confRequests.resultMap.entrySet()) {
      sendReply(entry.getKey(), entry.getValue());
    }
    if (confRequests.pendingRequest != null) {
      sendNotLeaderException(confRequests.pendingRequest);
    }
  }
}
