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
package org.apache.raft.grpc.server;

import com.google.common.base.Preconditions;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.netty.NettyServerBuilder;
import org.apache.raft.conf.RaftProperties;
import org.apache.raft.grpc.RaftGrpcConfigKeys;
import org.apache.raft.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.raft.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.raft.proto.RaftProtos.InstallSnapshotReplyProto;
import org.apache.raft.proto.RaftProtos.InstallSnapshotRequestProto;
import org.apache.raft.proto.RaftProtos.RequestVoteReplyProto;
import org.apache.raft.proto.RaftProtos.RequestVoteRequestProto;
import org.apache.raft.protocol.RaftPeer;
import org.apache.raft.server.RaftServer;
import org.apache.raft.server.RaftServerRpc;
import org.apache.raft.server.RequestDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.raft.grpc.RaftGrpcConfigKeys.RAFT_GRPC_SERVER_PORT_DEFAULT;
import static org.apache.raft.grpc.RaftGrpcConfigKeys.RAFT_GRPC_SERVER_PORT_KEY;

public class RaftGRpcService implements RaftServerRpc {
  static final Logger LOG = LoggerFactory.getLogger(RaftGRpcService.class);

  private final Server server;
  private final InetSocketAddress address;
  private final Map<String, RaftServerProtocolClient> peers =
      Collections.synchronizedMap(new HashMap<>());

  public RaftGRpcService(RaftServer raftServer, RaftProperties properties) {
    int port = properties.getInt(RAFT_GRPC_SERVER_PORT_KEY,
        RAFT_GRPC_SERVER_PORT_DEFAULT);
    int maxMessageSize = properties.getInt(
        RaftGrpcConfigKeys.RAFT_GRPC_MESSAGE_MAXSIZE_KEY,
        RaftGrpcConfigKeys.RAFT_GRPC_MESSAGE_MAXSIZE_DEFAULT);
    ServerBuilder serverBuilder = ServerBuilder.forPort(port);
    server = ((NettyServerBuilder) serverBuilder).maxMessageSize(maxMessageSize)
        .addService(new RaftServerProtocolService(new RequestDispatcher(raftServer)))
        .build();
    address = new InetSocketAddress(server.getPort());
    raftServer.setServerRpc(this);
  }

  @Override
  public void start() {
    try {
      server.start();
    } catch (IOException e) {
      LOG.error("Failed to start Grpc server on " + address.getPort(), e);
      System.exit(1);
    }
    LOG.info("Server started, listening on " + address.getPort());
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        System.err.println("*** shutting down gRPC server since JVM is shutting down");
        RaftGRpcService.this.shutdown();
        System.err.println("*** server shut down");
      }
    });
  }

  @Override
  public void shutdown() {
    if (server != null) {
      server.shutdown();
    }
    shutdownClients();
  }

  @Override
  public InetSocketAddress getInetSocketAddress() {
    return address;
  }

  @Override
  public AppendEntriesReplyProto sendAppendEntries(
      AppendEntriesRequestProto request) throws IOException {
    throw new UnsupportedOperationException(
        "Blocking AppendEntries call is not supported");
  }

  @Override
  public InstallSnapshotReplyProto sendInstallSnapshot(
      InstallSnapshotRequestProto request) throws IOException {
    throw new UnsupportedOperationException(
        "Blocking InstallSnapshot call is not supported");
  }

  @Override
  public RequestVoteReplyProto sendRequestVote(RequestVoteRequestProto request)
      throws IOException {
    RaftServerProtocolClient target = Preconditions.checkNotNull(
        peers.get(request.getServerRequest().getReplyId()));
    return target.requestVote(request);
  }

  @Override
  public void addPeerProxies(Iterable<RaftPeer> newPeers) {
    for (RaftPeer p : newPeers) {
      if (!peers.containsKey(p.getId())) {
        peers.put(p.getId(), new RaftServerProtocolClient(p));
      }
    }
  }

  private void shutdownClients() {
    peers.values().forEach(RaftServerProtocolClient::shutdown);
  }
}
