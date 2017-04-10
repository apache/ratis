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
package org.apache.ratis.server.simulation;

import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.RaftRpcMessage;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.IOUtils;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.Timestamp;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

class SimulatedRequestReply<REQUEST extends RaftRpcMessage,
    REPLY extends RaftRpcMessage> {
  public static final String SIMULATE_LATENCY_KEY
      = SimulatedRequestReply.class.getName() + ".simulateLatencyMs";
  public static final int SIMULATE_LATENCY_DEFAULT
      = RaftServerConfigKeys.Rpc.TIMEOUT_MIN_DEFAULT.toInt(TimeUnit.MILLISECONDS);
  public static final long TIMEOUT = 3000L;

  private static class ReplyOrException<REPLY> {
    private final REPLY reply;
    private final IOException ioe;

    ReplyOrException(REPLY reply, IOException ioe) {
      Preconditions.assertTrue(reply == null ^ ioe == null);
      this.reply = reply;
      this.ioe = ioe;
    }
  }

  static class EventQueue<REQUEST, REPLY> {
    private final BlockingQueue<REQUEST> requestQueue
        = new LinkedBlockingQueue<>();
    private final Map<REQUEST, ReplyOrException<REPLY>> replyMap
        = new ConcurrentHashMap<>();

    /** Block takeRequest for the requests sent from this server. */
    final AtomicBoolean blockTakeRequestFrom = new AtomicBoolean();
    /** Block sendRequest for the requests sent to this server. */
    final AtomicBoolean blockSendRequestTo = new AtomicBoolean();
    /** Delay takeRequest for the requests sent to this server. */
    final AtomicInteger delayTakeRequestTo = new AtomicInteger();
    /** Delay takeRequest for the requests sent from this server. */
    final AtomicInteger delayTakeRequestFrom = new AtomicInteger();

    REPLY request(REQUEST request) throws InterruptedException, IOException {
      requestQueue.put(request);
      synchronized (this) {
        final Timestamp startTime = new Timestamp();
        while (startTime.elapsedTimeMs() < TIMEOUT &&
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

  private final Map<String, EventQueue<REQUEST, REPLY>> queues
      = new ConcurrentHashMap<>();
  private final int simulateLatencyMs;

  SimulatedRequestReply(int simulateLatencyMs) {
    this.simulateLatencyMs = simulateLatencyMs;
  }

  EventQueue<REQUEST, REPLY> getQueue(String qid) {
    return queues.get(qid);
  }

  public REPLY sendRequest(REQUEST request) throws IOException {
    final String qid = request.getReplierId();
    final EventQueue<REQUEST, REPLY> q = queues.get(qid);
    if (q == null) {
      throw new IOException("The peer " + qid + " is not alive.");
    }
    try {
      RaftTestUtil.block(q.blockSendRequestTo::get);
      return q.request(request);
    } catch (InterruptedException e) {
      throw IOUtils.toInterruptedIOException("", e);
    }
  }

  public REQUEST takeRequest(String qid) throws IOException {
    final EventQueue<REQUEST, REPLY> q = queues.get(qid);
    if (q == null) {
      throw new IOException("The RPC of " + qid + " has already shutdown.");
    }

    final REQUEST request;
    try {
      // delay request for testing
      RaftTestUtil.delay(q.delayTakeRequestTo::get);

      request = q.takeRequest();
      Preconditions.assertTrue(qid.equals(request.getReplierId()));

      // block request for testing
      final EventQueue<REQUEST, REPLY> reqQ = queues.get(request.getRequestorId());
      if (reqQ != null) {
        RaftTestUtil.delay(reqQ.delayTakeRequestFrom::get);
        RaftTestUtil.block(reqQ.blockTakeRequestFrom::get);
      }
    } catch (InterruptedException e) {
      throw IOUtils.toInterruptedIOException("", e);
    }
    return request;
  }

  public void sendReply(REQUEST request, REPLY reply, IOException ioe)
      throws IOException {
    if (reply != null) {
      Preconditions.assertTrue(
          request.getRequestorId().equals(reply.getRequestorId()));
      Preconditions.assertTrue(
          request.getReplierId().equals(reply.getReplierId()));
    }
    simulateLatency();
    final String qid = request.getReplierId();
    EventQueue<REQUEST, REPLY> q = queues.get(qid);
    if (q != null) {
      q.reply(request, reply, ioe);
    }
  }

  public void shutdown(String id) {
    queues.remove(id);
  }

  public void clear() {
    queues.clear();
  }

  public void addPeer(RaftPeerId newPeer) {
    queues.put(newPeer.toString(), new EventQueue<>());
  }

  private void simulateLatency() throws IOException {
    if (simulateLatencyMs > 0) {
      int waitExpetation = simulateLatencyMs / 10;
      int waitHalfRange = waitExpetation / 3;
      int randomSleepMs = ThreadLocalRandom.current().nextInt(2 * waitHalfRange)
          + waitExpetation - waitHalfRange;
      try {
        Thread.sleep(randomSleepMs);
      } catch (InterruptedException ie) {
        throw IOUtils.toInterruptedIOException("", ie);
      }
    }
  }
}
