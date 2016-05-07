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
import org.apache.hadoop.raft.protocol.RaftRpcMessage;
import org.apache.hadoop.raft.server.RaftConfiguration;
import org.apache.hadoop.raft.server.RaftRpc;
import org.apache.hadoop.raft.server.protocol.RaftPeer;

import java.io.InterruptedIOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class SimulatedRpc<REQUEST extends RaftRpcMessage,
    REPLY extends RaftRpcMessage> implements RaftRpc<REQUEST, REPLY> {
  static class EventQueue<REQUEST, REPLY> {
    private final BlockingQueue<REQUEST> requestQueue;
    private final Map<REQUEST, REPLY> replyMap;

    EventQueue() {
      this.requestQueue = new LinkedBlockingQueue<>();
      this.replyMap = new ConcurrentHashMap<>();
    }

    REPLY request(REQUEST request)
        throws InterruptedException {
      requestQueue.add(request);
      synchronized (this) {
        while (!replyMap.containsKey(request)) {
          this.wait();
        }
      }
      return replyMap.remove(request);
    }

    REQUEST takeRequest() throws InterruptedException {
      return requestQueue.take();
    }

    void reply(REQUEST request, REPLY reply) {
      replyMap.put(request, reply);
      synchronized (this) {
        this.notifyAll();
      }
    }
  }

  private final Map<String, EventQueue<REQUEST, REPLY>> queues;

  public SimulatedRpc(RaftConfiguration conf) {
    Map<String, EventQueue<REQUEST, REPLY>> map = new HashMap<>();
    for (RaftPeer peer : conf.getPeers()) {
      map.put(peer.getId(), new EventQueue());
    }
    queues = Collections.unmodifiableMap(map);
  }

  @Override
  public REPLY sendRequest(REQUEST request) throws InterruptedIOException {
    final String qid = request.getReplierId();
    final EventQueue<REQUEST, REPLY> q = queues.get(qid);
    try {
      return q.request(request);
    } catch (InterruptedException e) {
      throw toInterruptedIOException("", e);
    }
  }

  @Override
  public REQUEST takeRequest(String qid) throws InterruptedIOException {
    try {
      return queues.get(qid).takeRequest();
    } catch (InterruptedException e) {
      throw toInterruptedIOException("", e);
    }
  }

  @Override
  public void sendReply(REQUEST request, REPLY reply) {
    Preconditions.checkArgument(
        request.getRequestorId().equals(reply.getRequestorId()));
    Preconditions.checkArgument(
        request.getReplierId().equals(reply.getReplierId()));

    final String qid = request.getReplierId();
    queues.get(qid).reply(request, reply);
  }

  static InterruptedIOException toInterruptedIOException(
      String message, InterruptedException e) {
    final InterruptedIOException iioe = new InterruptedIOException(message);
    iioe.initCause(e);
    return iioe;
  }
}
