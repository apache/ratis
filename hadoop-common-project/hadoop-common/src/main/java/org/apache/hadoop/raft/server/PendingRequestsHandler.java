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
import org.apache.hadoop.util.Daemon;
import org.slf4j.Logger;

import java.io.IOException;
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

  private final RaftServer server;
  private final PriorityBlockingQueue<PendingRequestElement> queue =
      new PriorityBlockingQueue<>();
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

  void put(long index, RaftClientRequest request) {
    final boolean b = queue.offer(new PendingRequestElement(index, request));
    Preconditions.checkState(b);
  }

  class PendingRequestDaemon extends Daemon {
    @Override
    public String toString() {
      return getClass().getSimpleName();
    }

    @Override
    public void run() {
      long lastCommitted = 0;
      while (running) {
        try {
          lastCommitted = server.getState().getLog()
              .waitLastCommitted(lastCommitted);
          for (PendingRequestElement pre;
               (pre = queue.peek()) != null && pre.index <= lastCommitted; ) {
            pre = queue.poll();
            try {
              server.clientHandler.sendReply(pre.request,
                  new RaftClientReply(pre.request), null);
            } catch (IOException ioe) {
              RaftServer.LOG.error(this + " has " + ioe);
              RaftServer.LOG.trace("TRACE", ioe);
            }
          }
        } catch (InterruptedException e) {
          RaftServer.LOG.info(this + " is interrupted by " + e);
          RaftServer.LOG.trace("TRACE", e);
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
    while (!queue.isEmpty()) {
      final PendingRequestElement req = queue.poll();
      try {
        if (req.index <= lastCommitted) {
          server.clientHandler.sendReply(req.request,
              new RaftClientReply(req.request), null);
        } else {
          server.clientHandler.sendReply(req.request,
              null, server.generateNotLeaderException());
        }
      } catch (IOException e) {
        RaftServer.LOG.error("{} hit exception {} when sending" +
            " NotLeaderException to client request {}", this, e, req.request);
        RaftServer.LOG.trace("TRACE", e);
      }
    }
  }
}
