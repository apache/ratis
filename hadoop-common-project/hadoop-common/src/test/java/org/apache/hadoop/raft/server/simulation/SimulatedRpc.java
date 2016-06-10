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
package org.apache.hadoop.raft.server.simulation;

import com.google.common.base.Preconditions;
import org.apache.hadoop.raft.RaftUtils;
import org.apache.hadoop.raft.protocol.RaftPeer;
import org.apache.hadoop.raft.protocol.RaftRpcMessage;
import org.apache.hadoop.raft.server.RaftConstants;
import org.apache.hadoop.raft.server.RaftRpc;
import org.apache.hadoop.raft.server.protocol.AppendEntriesRequest;
import org.apache.mina.util.ConcurrentHashSet;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class SimulatedRpc<REQUEST extends RaftRpcMessage,
    REPLY extends RaftRpcMessage> implements RaftRpc<REQUEST, REPLY> {
  private static class ReplyOrException<REPLY> {
    private final REPLY reply;
    private final IOException ioe;

    ReplyOrException(REPLY reply, IOException ioe) {
      Preconditions.checkArgument(reply == null ^ ioe == null);
      this.reply = reply;
      this.ioe = ioe;
    }
  }

  static class EventQueue<REQUEST, REPLY> {
    private final BlockingQueue<REQUEST> requestQueue;
    private final Map<REQUEST, ReplyOrException<REPLY>> replyMap;
    private volatile boolean enabled = true;
    private volatile int takeRequestDelayMs = 0;

    EventQueue() {
      this.requestQueue = new LinkedBlockingQueue<>();
      this.replyMap = new ConcurrentHashMap<>();
    }

    REPLY request(REQUEST request) throws InterruptedException, IOException {
      while (!enabled) {
        Thread.sleep(RaftConstants.ELECTION_TIMEOUT_MAX_MS);
      }
      requestQueue.put(request);
      synchronized (this) {
        while (!replyMap.containsKey(request)) {
          this.wait();
        }
      }

      final ReplyOrException<REPLY> re = replyMap.remove(request);
      if (re.ioe != null) {
        throw re.ioe;
      }
      return re.reply;
    }

    REQUEST takeRequest() throws InterruptedException {
      Thread.sleep(takeRequestDelayMs);
      return requestQueue.take();
    }

    void reply(REQUEST request, REPLY reply, IOException ioe) {
      replyMap.put(request, new ReplyOrException<>(reply, ioe));
      synchronized (this) {
        this.notifyAll();
      }
    }

    void setEnabled(boolean enabled) {
      this.enabled = enabled;
    }

    boolean getEnabled() {
      return enabled;
    }

    int getTakeRequestDelayMs() {
      return takeRequestDelayMs;
    }

    void setTakeRequestDelayMs(int takeRequestDelayMs) {
      this.takeRequestDelayMs = takeRequestDelayMs;
    }
  }

  static final String BLACKLIST_SEPARATOR = ":";

  private final Map<String, EventQueue<REQUEST, REPLY>> queues;
  private final Set<String> blacklist;

  public SimulatedRpc(Collection<RaftPeer> allPeers) {
    queues = new ConcurrentHashMap<>();
    for (RaftPeer peer : allPeers) {
      queues.put(peer.getId(), new EventQueue<REQUEST, REPLY>());
    }
    blacklist = new ConcurrentHashSet<>();
  }

  @Override
  public REPLY sendRequest(REQUEST request) throws IOException {
    final String qid = request.getReplierId();
    final EventQueue<REQUEST, REPLY> q = queues.get(qid);
    try {
      return q.request(request);
    } catch (InterruptedException e) {
      throw RaftUtils.toInterruptedIOException("", e);
    }
  }

  @Override
  public REQUEST takeRequest(String qid) throws InterruptedIOException {
    try {
      REQUEST request = queues.get(qid).takeRequest();
      final String rid = request.getRequestorId();
      if (!(request instanceof AppendEntriesRequest)
          || ((AppendEntriesRequest) request).getEntries() != null) {
        while (isBlacklisted(rid, qid)) {
          Thread.sleep(RaftConstants.ELECTION_TIMEOUT_MAX_MS + 100);
        }
      }
      return request;
    } catch (InterruptedException e) {
      throw RaftUtils.toInterruptedIOException("", e);
    }
  }

  @Override
  public void sendReply(REQUEST request, REPLY reply, IOException ioe) {
    if (reply != null) {
      Preconditions.checkArgument(
          request.getRequestorId().equals(reply.getRequestorId()));
      Preconditions.checkArgument(
          request.getReplierId().equals(reply.getReplierId()));
    }

    final String qid = request.getReplierId();
    queues.get(qid).reply(request, reply, ioe);
  }

  public void addPeers(Collection<RaftPeer> newPeers) {
    for (RaftPeer peer : newPeers) {
      queues.put(peer.getId(), new EventQueue<REQUEST, REPLY>());
    }
  }

  // Utility methods for testing
  public boolean isQueueEnabled(String qid) {
    return queues.get(qid).getEnabled();
  }

  public void setQueueEnabled(String qid, boolean enabled) {
    queues.get(qid).setEnabled(enabled);
  }

  public int getTakeRequestDelayMs(String qid) {
    return queues.get(qid).getTakeRequestDelayMs();
  }

  public void setTakeRequestDelayMs(String qid, int delayMs) {
    queues.get(qid).setTakeRequestDelayMs(delayMs);
  }

  public void addBlacklist(String src, String[] dsts) {
    for (String dst : dsts) {
      blacklist.add(src + BLACKLIST_SEPARATOR + dst);
    }
  }

  public void removeBlacklist(String src, String dst) {
    blacklist.remove(src + BLACKLIST_SEPARATOR + dst);
  }

  public void removeBlacklist(String src, String[] dsts) {
    for (String dst : dsts) {
      removeBlacklist(src, dsts);
    }
  }

  public boolean isBlacklisted(String src, String dst) {
    return blacklist.contains(src + BLACKLIST_SEPARATOR + dst);
  }
}
