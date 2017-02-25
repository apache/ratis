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
package org.apache.ratis.grpc;

import com.google.common.base.Preconditions;
import org.apache.ratis.RpcType;
import org.apache.ratis.grpc.client.RaftClientProtocolService;
import org.apache.ratis.grpc.server.RaftServerProtocolClient;
import org.apache.ratis.grpc.server.RaftServerProtocolService;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerRpc;
import org.apache.ratis.shaded.io.grpc.Server;
import org.apache.ratis.shaded.io.grpc.ServerBuilder;
import org.apache.ratis.shaded.io.grpc.netty.NettyServerBuilder;
import org.apache.ratis.shaded.proto.RaftProtos.*;
import org.apache.ratis.util.CodeInjectionForTesting;
import org.apache.ratis.util.ExitUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.ratis.grpc.RaftGrpcConfigKeys.*;

/** A grpc implementation of {@link RaftServerRpc}. */
public class RaftGRpcService implements RaftServerRpc {
  static final Logger LOG = LoggerFactory.getLogger(RaftGRpcService.class);
  public static final String GRPC_SEND_SERVER_REQUEST =
      RaftGRpcService.class.getSimpleName() + ".sendRequest";

  public static class Builder extends RaftServerRpc.Builder<Builder,RaftGRpcService> {
    private Builder() {}

    @Override
    public Builder getThis() {
      return this;
    }

    @Override
    public RaftGRpcService build() {
      return new RaftGRpcService(getServer());
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  private final Server server;
  private final InetSocketAddress address;
  private final Map<RaftPeerId, RaftServerProtocolClient> peers =
      Collections.synchronizedMap(new HashMap<>());
  private final RaftPeerId selfId;

  private RaftGRpcService(RaftServer server) {
    this(server,
        server.getProperties().getInt(RAFT_GRPC_SERVER_PORT_KEY,
            RAFT_GRPC_SERVER_PORT_DEFAULT),
        server.getProperties().getInt(RAFT_GRPC_MESSAGE_MAXSIZE_KEY,
            RAFT_GRPC_MESSAGE_MAXSIZE_DEFAULT));
  }
  private RaftGRpcService(RaftServer raftServer, int port, int maxMessageSize) {
    ServerBuilder serverBuilder = ServerBuilder.forPort(port);
    selfId = raftServer.getId();
    server = ((NettyServerBuilder) serverBuilder).maxMessageSize(maxMessageSize)
        .addService(new RaftServerProtocolService(selfId, raftServer))
        .addService(new RaftClientProtocolService(selfId, raftServer))
        .build();

    // start service to determine the port (in case port is configured as 0)
    startService();
    address = new InetSocketAddress(server.getPort());
    LOG.info("Server started, listening on " + address.getPort());
  }

  @Override
  public RpcType getRpcType() {
    return RpcType.GRPC;
  }

  @Override
  public void start() {
    // do nothing
  }

  private void startService() {
    try {
      server.start();
    } catch (IOException e) {
      ExitUtils.terminate(1, "Failed to start Grpc server", e, LOG);
    }
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      System.err.println("*** shutting down gRPC server since JVM is shutting down");
      RaftGRpcService.this.close();
      System.err.println("*** server shut down");
    }));
  }

  @Override
  public void close() {
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
  public AppendEntriesReplyProto appendEntries(
      AppendEntriesRequestProto request) throws IOException {
    throw new UnsupportedOperationException(
        "Blocking AppendEntries call is not supported");
  }

  @Override
  public InstallSnapshotReplyProto installSnapshot(
      InstallSnapshotRequestProto request) throws IOException {
    throw new UnsupportedOperationException(
        "Blocking InstallSnapshot call is not supported");
  }

  @Override
  public RequestVoteReplyProto requestVote(RequestVoteRequestProto request)
      throws IOException {
    CodeInjectionForTesting.execute(GRPC_SEND_SERVER_REQUEST, selfId,
        null, request);

    RaftServerProtocolClient target = Preconditions.checkNotNull(
        peers.get(new RaftPeerId(request.getServerRequest().getReplyId())));
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
