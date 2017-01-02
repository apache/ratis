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
package org.apache.raft.grpc;

import com.google.common.base.Preconditions;
import org.apache.raft.conf.RaftProperties;
import org.apache.raft.grpc.client.RaftClientProtocolService;
import org.apache.raft.grpc.server.RaftServerProtocolClient;
import org.apache.raft.grpc.server.RaftServerProtocolService;
import org.apache.raft.protocol.RaftPeer;
import org.apache.raft.server.RaftServer;
import org.apache.raft.server.RaftServerRpc;
import org.apache.raft.server.impl.RequestDispatcher;
import org.apache.raft.shaded.io.grpc.Server;
import org.apache.raft.shaded.io.grpc.ServerBuilder;
import org.apache.raft.shaded.io.grpc.netty.NettyServerBuilder;
import org.apache.raft.shaded.proto.RaftProtos.*;
import org.apache.raft.util.CodeInjectionForTesting;
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
  public static final String GRPC_SEND_SERVER_REQUEST =
      RaftGRpcService.class.getSimpleName() + ".sendRequest";

  private final Server server;
  private final InetSocketAddress address;
  private final Map<String, RaftServerProtocolClient> peers =
      Collections.synchronizedMap(new HashMap<>());
  private final String selfId;

  public RaftGRpcService(RaftServer raftServer, RaftProperties properties) {
    int port = properties.getInt(RAFT_GRPC_SERVER_PORT_KEY,
        RAFT_GRPC_SERVER_PORT_DEFAULT);
    int maxMessageSize = properties.getInt(
        RaftGrpcConfigKeys.RAFT_GRPC_MESSAGE_MAXSIZE_KEY,
        RaftGrpcConfigKeys.RAFT_GRPC_MESSAGE_MAXSIZE_DEFAULT);
    ServerBuilder serverBuilder = ServerBuilder.forPort(port);
    final RequestDispatcher dispatcher = new RequestDispatcher(raftServer);
    selfId = raftServer.getId();
    server = ((NettyServerBuilder) serverBuilder).maxMessageSize(maxMessageSize)
        .addService(new RaftServerProtocolService(selfId, dispatcher))
        .addService(new RaftClientProtocolService(selfId, dispatcher))
        .build();

    // start service to determine the port (in case port is configured as 0)
    startService();
    address = new InetSocketAddress(server.getPort());
    LOG.info("Server started, listening on " + address.getPort());
  }

  @Override
  public void start() {
    // do nothing
  }

  private void startService() {
    try {
      server.start();
    } catch (IOException e) {
      LOG.error("Failed to start Grpc server", e);
      System.exit(1);
    }
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
    CodeInjectionForTesting.execute(GRPC_SEND_SERVER_REQUEST, selfId,
        null, request);

    RaftServerProtocolClient target = Preconditions.checkNotNull(
        peers.get(request.getServerRequest().getReplyId()));
    return target.requestVote(request);
  }

  @Override
  public void addPeers(Iterable<RaftPeer> newPeers) {
    for (RaftPeer p : newPeers) {
      if (!peers.containsKey(p.getId())) {
        peers.put(p.getId(), new RaftServerProtocolClient(p));
      }
    }
  }

  private void shutdownClients() {
    peers.values().forEach(RaftServerProtocolClient::shutdown);
  }

  public RaftServerProtocolClient getRpcClient(RaftPeer peer) {
    return peers.get(peer.getId());
  }
}
