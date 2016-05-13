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
import org.apache.hadoop.raft.protocol.RaftRpcMessage;
import org.apache.hadoop.raft.server.RaftRpc;
import org.apache.hadoop.raft.protocol.RaftPeer;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
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

    EventQueue() {
      this.requestQueue = new LinkedBlockingQueue<>();
      this.replyMap = new ConcurrentHashMap<>();
    }

    REPLY request(REQUEST request) throws InterruptedException, IOException {
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
      return requestQueue.take();
    }

    void reply(REQUEST request, REPLY reply, IOException ioe) {
      replyMap.put(request, new ReplyOrException<>(reply, ioe));
      synchronized (this) {
        this.notifyAll();
      }
    }
  }

  private final Map<String, EventQueue<REQUEST, REPLY>> queues;

  public SimulatedRpc(Collection<RaftPeer> allPeers) {
    queues = new ConcurrentHashMap<>();
    for (RaftPeer peer : allPeers) {
      queues.put(peer.getId(), new EventQueue<REQUEST, REPLY>());
    }
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
      return queues.get(qid).takeRequest();
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
}
