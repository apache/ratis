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
package org.apache.raft.grpc.client;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.raft.client.ClientProtoUtils;
import org.apache.raft.client.RaftClient;
import org.apache.raft.conf.RaftProperties;
import org.apache.raft.grpc.RaftGrpcUtil;
import org.apache.raft.proto.RaftProtos.RaftClientReplyProto;
import org.apache.raft.proto.RaftProtos.RaftClientRequestProto;
import org.apache.raft.protocol.NotLeaderException;
import org.apache.raft.protocol.RaftClientReply;
import org.apache.raft.protocol.RaftPeer;
import org.apache.raft.util.RaftUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.raft.grpc.RaftGrpcConfigKeys.RAFT_GRPC_CLIENT_MAX_OUTSTANDING_APPENDS_DEFAULT;
import static org.apache.raft.grpc.RaftGrpcConfigKeys.RAFT_GRPC_CLIENT_MAX_OUTSTANDING_APPENDS_KEY;

class AppendStreamer implements Closeable {
  static final Logger LOG = LoggerFactory.getLogger(AppendStreamer.class);

  private static class RaftPeerAndProxy {
    private final RaftPeer peer;
    private RaftClientProtocolClient proxy;

    RaftPeerAndProxy(RaftPeer peer) {
      this.peer = peer;
    }

    synchronized RaftClientProtocolClient getProxy() {
      if (proxy == null) {
        proxy = new RaftClientProtocolClient(peer);
      }
      return proxy;
    }

    void close() {
      if (proxy != null) {
        proxy.close();
      }
    }
  }

  private volatile StreamObserver<RaftClientRequestProto> requestObserver;
  private final Queue<RaftClientRequestProto> pendingRequests;
  private final int maxPendingNum;

  private final Map<String, RaftPeerAndProxy> peers;
  private volatile String leaderId;
  private final String clientId;

  private volatile boolean running = true;

  AppendStreamer(RaftProperties prop, Collection<RaftPeer> peers,
      String leaderId, String clientId) {
    this.clientId = clientId;
    this.maxPendingNum = prop.getInt(
        RAFT_GRPC_CLIENT_MAX_OUTSTANDING_APPENDS_KEY,
        RAFT_GRPC_CLIENT_MAX_OUTSTANDING_APPENDS_DEFAULT);
    this.pendingRequests = new LinkedList<>();

    this.peers = peers.stream().collect(
        Collectors.toMap(RaftPeer::getId, RaftPeerAndProxy::new));
    refreshLeaderProxy(leaderId, null);
  }

  private void refreshLeaderProxy(String suggested, String oldLeader) {
    if (suggested != null) {
      leaderId = suggested;
    } else {
      if (oldLeader == null) {
        leaderId = peers.keySet().iterator().next();
      } else {
        leaderId = RaftClient.nextLeader(oldLeader, peers.keySet().iterator());
      }
    }
    requestObserver = peers.get(suggested).getProxy()
        .append(new ResponseHandler(leaderId));
  }

  private void checkState() throws IOException {
    if (!running) {
      throw new IOException("The AppendStreamer has been closed");
    }
  }

  synchronized void write(ByteString content, long seqNum)
      throws IOException {
    checkState();
    while (running && pendingRequests.size() >= maxPendingNum) {
      try {
        wait();
      } catch (InterruptedException ignored) {
      }
    }
    // wrap the current buffer into a RaftClientRequestProto
    final RaftClientRequestProto request = ClientProtoUtils
        .genRaftClientRequestProto(clientId, leaderId, seqNum, content, false);
    if (running) {
      requestObserver.onNext(request);
      pendingRequests.offer(request);
    } else {
      throw new IOException(this + " got closed");
    }
  }

  synchronized void flush() throws IOException {
    checkState();
    if (pendingRequests.isEmpty()) {
      return;
    }
    // wait for the pending Q to become empty
    while (running && !pendingRequests.isEmpty()) {
      try {
        wait();
      } catch (InterruptedException ignored) {
      }
    }
    if (!running && !pendingRequests.isEmpty()) {
      throw new IOException(this + " got closed before finishing flush");
    }
  }

  @Override
  public synchronized void close() throws IOException {
    if (!running) {
      return;
    }
    requestObserver.onCompleted();

    flush();
    running = false;
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + "-" + clientId;
  }

  /** the response handler for stream RPC */
  private class ResponseHandler implements StreamObserver<RaftClientReplyProto> {
    private final String leaderId;

    ResponseHandler(String leaderId) {
      this.leaderId = leaderId;
    }

    @Override
    public String toString() {
      return AppendStreamer.this + "-ResponseHandler-" + leaderId;
    }

    @Override
    public void onNext(RaftClientReplyProto reply) {
      synchronized (AppendStreamer.this) {
        RaftClientRequestProto pending = Preconditions.checkNotNull(
            pendingRequests.peek());
        if (reply.getRpcReply().getSuccess()) {
          Preconditions.checkState(pending.getRpcRequest().getSeqNum() ==
              reply.getRpcReply().getSeqNum());
          pendingRequests.poll();
        } else {
          // this may be a NotLeaderException
          RaftClientReply r = ClientProtoUtils.toRaftClientReply(reply);
          if (r.isNotLeader()) {
            if (requestObserver != null) {
              requestObserver.onCompleted();
            }
            // handle NotLeaderException: refresh leader and RaftConfiguration
            final NotLeaderException nle = r.getNotLeaderException();

            refreshPeers(nle.getPeers());
            refreshLeaderProxy(nle.getSuggestedLeader().getId(), leaderId);
            resendPendingRequests();
          }
        }
        AppendStreamer.this.notify();
      }
    }

    @Override
    public void onError(Throwable t) {
      handleError(t);
    }

    @Override
    public void onCompleted() {
      LOG.info("{} onCompleted", this);
      Preconditions.checkState(pendingRequests.isEmpty());
    }
  }

  private void handleError(Throwable t) {
    final IOException e;
    if (t instanceof StatusRuntimeException) {
      e = RaftGrpcUtil.unwrapException((StatusRuntimeException) t);
    } else {
      e = RaftUtils.asIOException(t);
    }
    LOG.warn("{} got error: {}", this, e);
    synchronized (AppendStreamer.this) {
      // TODO add upper limit for total retry numbers: if exceeded, set the exception and close the streamer
      refreshLeaderProxy(null, leaderId);
      resendPendingRequests();
      AppendStreamer.this.notify();
    }
  }

  private void resendPendingRequests() {
    // resend all the pending requests
    for (RaftClientRequestProto request : pendingRequests) {
      requestObserver.onNext(request);
    }
  }

  private void refreshPeers(RaftPeer[] newPeers) {
    if (newPeers != null && newPeers.length > 0) {
      Map<String, RaftPeer> newPeersMap = Arrays.stream(newPeers)
          .collect(Collectors.toMap(RaftPeer::getId, Function.identity()));

      Iterator<Map.Entry<String, RaftPeerAndProxy>> iter =
          peers.entrySet().iterator();
      while (iter.hasNext()) {
        Map.Entry<String, RaftPeerAndProxy> entry = iter.next();
        if (!newPeersMap.containsKey(entry.getKey())) {
          entry.getValue().close();
          iter.remove();
        }
      }

      newPeersMap.entrySet().forEach(entry ->
          peers.putIfAbsent(entry.getKey(),
              new RaftPeerAndProxy(entry.getValue())));
    }
  }
}
