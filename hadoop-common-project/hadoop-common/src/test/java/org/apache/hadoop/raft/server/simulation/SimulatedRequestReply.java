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
import org.apache.hadoop.raft.util.RaftUtils;
import org.apache.hadoop.raft.protocol.RaftPeer;
import org.apache.hadoop.raft.protocol.RaftRpcMessage;
import org.apache.hadoop.raft.server.RaftConstants;
import org.apache.hadoop.raft.server.RequestReply;
import org.apache.hadoop.raft.server.protocol.AppendEntriesRequest;
import org.apache.hadoop.util.Time;
import org.apache.mina.util.ConcurrentHashSet;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class SimulatedRpc<REQUEST extends RaftRpcMessage,
    REPLY extends RaftRpcMessage> implements RequestReply<REQUEST, REPLY> {
  public static final long TIMEOUT = 3000L;

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
    private volatile boolean openForMessage = true;
    private volatile int takeRequestDelayMs = 0;

    EventQueue() {
      this.requestQueue = new LinkedBlockingQueue<>();
      this.replyMap = new ConcurrentHashMap<>();
    }

    REPLY request(REQUEST request) throws InterruptedException, IOException {
      requestQueue.put(request);
      synchronized (this) {
        final long startTime = Time.monotonicNow();
        while (Time.monotonicNow() - startTime < TIMEOUT &&
            !replyMap.containsKey(request)) {
          this.wait(TIMEOUT); // no need to be precise here
        }
      }

      if (!replyMap.containsKey(request)) {
        throw new IOException("Timeout while waiting for reply of request "
            + request);
      }
      final ReplyOrException<REPLY> re = replyMap.remove(request);
      if (re.ioe != null) {
        throw re.ioe;
      }
      return re.reply;
    }

    REQUEST takeRequest() throws InterruptedException {
      return requestQueue.take();
    }

    void reply(REQUEST request, REPLY reply, IOException ioe)
        throws IOException {
      replyMap.put(request, new ReplyOrException<>(reply, ioe));
      synchronized (this) {
        this.notifyAll();
      }
    }
  }

  static final String BLACKLIST_SEPARATOR = ":";

  private final Map<String, EventQueue<REQUEST, REPLY>> queues;
  private final Set<String> blacklist;

  public SimulatedRpc(Collection<RaftPeer> allPeers) {
    queues = new ConcurrentHashMap<>();
    for (RaftPeer peer : allPeers) {
      queues.put(peer.getId(), new EventQueue<>());
    }
    blacklist = new ConcurrentHashSet<>();
  }

  @Override
  public REPLY sendRequest(REQUEST request) throws IOException {
    final String qid = request.getReplierId();
    final EventQueue<REQUEST, REPLY> q = queues.get(qid);
    if (q == null) {
      throw new IOException("The peer " + qid + " is not alive.");
    }
    try {
      blockForReceiverQueue(qid);
      return q.request(request);
    } catch (InterruptedException e) {
      throw RaftUtils.toInterruptedIOException("", e);
    }
  }

  @Override
  public REQUEST takeRequest(String qid) throws IOException {
    try {
      final EventQueue<REQUEST, REPLY> q = queues.get(qid);
      if (q == null) {
        throw new IOException("The RPC of " + qid + " has already shutdown.");
      }
      delayBeforeTake(qid);
      REQUEST request = q.takeRequest();
      blockForBlacklist(request, qid);
      return request;
    } catch (InterruptedException e) {
      throw RaftUtils.toInterruptedIOException("", e);
    }
  }

  @Override
  public void sendReply(REQUEST request, REPLY reply, IOException ioe)
      throws IOException {
    if (reply != null) {
      Preconditions.checkArgument(
          request.getRequestorId().equals(reply.getRequestorId()));
      Preconditions.checkArgument(
          request.getReplierId().equals(reply.getReplierId()));
    }
    simulateLatency();
    final String qid = request.getReplierId();
    EventQueue<REQUEST, REPLY> q = queues.get(qid);
    if (q != null) {
      q.reply(request, reply, ioe);
    }
  }

  @Override
  public void shutdown(String id) throws IOException {
    queues.remove(id);
  }

  public void addPeers(Collection<RaftPeer> newPeers) {
    for (RaftPeer peer : newPeers) {
      queues.put(peer.getId(), new EventQueue<>());
    }
  }

  protected void blockForReceiverQueue(String qid)
      throws IOException {
    EventQueue queue = queues.get(qid);
    try {
      while (!queue.openForMessage) {
        Thread.sleep(RaftConstants.ELECTION_TIMEOUT_MAX_MS);
      }
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    }
  }

  protected void blockForBlacklist(REQUEST request, String dstId)
      throws IOException {
    final String rid = request.getRequestorId();
    if (!(request instanceof AppendEntriesRequest)
        || ((AppendEntriesRequest) request).getEntries() != null) {
      try {
        while (isBlacklisted(rid, dstId)) {
          Thread.sleep(RaftConstants.ELECTION_TIMEOUT_MAX_MS + 100);
        }
      } catch (InterruptedException ie) {
        throw new IOException(ie);
      }
    }
  }

  protected void simulateLatency() throws IOException {
    int waitExpetation = RaftConstants.ELECTION_TIMEOUT_MIN_MS / 10;
    int waitHalfRange = waitExpetation / 3;
    Random rand = new Random();
    int randomSleepMs = rand.nextInt(2 * waitHalfRange)
        + waitExpetation - waitHalfRange;
    try {
      Thread.sleep(randomSleepMs);
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    }
  }

  protected void delayBeforeTake(String qid) throws IOException {
    EventQueue queue = queues.get(qid);
    if (queue.takeRequestDelayMs > 0) {
      try {
        Thread.sleep(queue.takeRequestDelayMs);
      } catch (InterruptedException ie) {
        throw new IOException(ie);
      }
    }
  }

  // Utility methods for testing
  public boolean isOpenForMessage(String qid) {
    return queues.get(qid).openForMessage;
  }

  public void setIsOpenForMessage(String qid, boolean enabled) {
    queues.get(qid).openForMessage = enabled;
  }

  public int getTakeRequestDelayMs(String qid) {
    return queues.get(qid).takeRequestDelayMs;
  }

  public void setTakeRequestDelayMs(String qid, int delayMs) {
    queues.get(qid).takeRequestDelayMs = delayMs;
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
      removeBlacklist(src, dst);
    }
  }

  public boolean isBlacklisted(String src, String dst) {
    return blacklist.contains(src + BLACKLIST_SEPARATOR + dst);
  }
}
