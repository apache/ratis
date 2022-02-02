/*
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

import org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotReplyProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotRequestProto;
import org.apache.ratis.proto.RaftProtos.RequestVoteReplyProto;
import org.apache.ratis.proto.RaftProtos.RequestVoteRequestProto;
import org.apache.ratis.proto.RaftProtos.StartLeaderElectionReplyProto;
import org.apache.ratis.proto.RaftProtos.StartLeaderElectionRequestProto;
import org.apache.ratis.protocol.GroupInfoRequest;
import org.apache.ratis.protocol.GroupListRequest;
import org.apache.ratis.protocol.GroupManagementRequest;
import org.apache.ratis.protocol.LeaderElectionManagementRequest;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.SetConfigurationRequest;
import org.apache.ratis.protocol.SnapshotManagementRequest;
import org.apache.ratis.protocol.TransferLeadershipRequest;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerRpc;
import org.apache.ratis.util.Daemon;
import org.apache.ratis.util.IOUtils;
import org.apache.ratis.util.JavaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

class SimulatedServerRpc implements RaftServerRpc {
  static final Logger LOG = LoggerFactory.getLogger(SimulatedServerRpc.class);

  private final RaftServer server;
  private final RequestHandler<RaftServerRequest, RaftServerReply> serverHandler;
  private final RequestHandler<RaftClientRequest, RaftClientReply> clientHandler;
  private final ExecutorService executor = Executors.newFixedThreadPool(3, Daemon::new);

  SimulatedServerRpc(RaftServer server,
      SimulatedRequestReply<RaftServerRequest, RaftServerReply> serverRequestReply,
      SimulatedRequestReply<RaftClientRequest, RaftClientReply> clientRequestReply) {
    this.server = server;

    final Supplier<String> id = () -> server.getId().toString();
    this.serverHandler = new RequestHandler<>(id, "serverHandler", serverRequestReply, serverHandlerImpl, 3);
    this.clientHandler = new RequestHandler<>(id, "clientHandler", clientRequestReply, clientHandlerImpl, 3);
  }

  @Override
  public SimulatedRpc getRpcType() {
    return SimulatedRpc.INSTANCE;
  }

  @Override
  public void start() {
    serverHandler.startDaemon();
    clientHandler.startDaemon();
  }

  private void interruptAndJoin() throws InterruptedException {
    clientHandler.interruptAndJoinDaemon();
    serverHandler.interruptAndJoinDaemon();
  }

  @Override
  public void close() {
    try {
      interruptAndJoin();
      executor.shutdown();
      executor.awaitTermination(1000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException ignored) {
      Thread.currentThread().interrupt();
    }
    clientHandler.shutdown();
    serverHandler.shutdown();
  }

  @Override
  public InetSocketAddress getInetSocketAddress() {
    return null;
  }

  @Override
  public AppendEntriesReplyProto appendEntries(AppendEntriesRequestProto request)
      throws IOException {
    RaftServerReply reply = serverHandler.getRpc()
        .sendRequest(new RaftServerRequest(request));
    return reply.getAppendEntries();
  }

  @Override
  public InstallSnapshotReplyProto installSnapshot(InstallSnapshotRequestProto request)
      throws IOException {
    RaftServerReply reply = serverHandler.getRpc()
        .sendRequest(new RaftServerRequest(request));
    return reply.getInstallSnapshot();
  }

  @Override
  public RequestVoteReplyProto requestVote(RequestVoteRequestProto request)
      throws IOException {
    RaftServerReply reply = serverHandler.getRpc()
        .sendRequest(new RaftServerRequest(request));
    return reply.getRequestVote();
  }

  @Override
  public StartLeaderElectionReplyProto startLeaderElection(StartLeaderElectionRequestProto request)
      throws IOException {
    RaftServerReply reply = serverHandler.getRpc().sendRequest(new RaftServerRequest(request));
    return reply.getStartLeaderElection();
  }

  @Override
  public void addRaftPeers(Collection<RaftPeer> peers) {
    // do nothing
  }

  @Override
  public void handleException(RaftPeerId serverId, Exception e, boolean reconnect) {
    // do nothing
  }

  final RequestHandler.HandlerInterface<RaftServerRequest, RaftServerReply> serverHandlerImpl
      = new RequestHandler.HandlerInterface<RaftServerRequest, RaftServerReply>() {
    @Override
    public boolean isAlive() {
      return !server.getLifeCycleState().isClosingOrClosed();
    }

    @Override
    public RaftServerReply handleRequest(RaftServerRequest r)
        throws IOException {
      if (r.isAppendEntries()) {
        return new RaftServerReply(server.appendEntries(r.getAppendEntries()));
      } else if (r.isRequestVote()) {
        return new RaftServerReply(server.requestVote(r.getRequestVote()));
      } else if (r.isInstallSnapshot()) {
        return new RaftServerReply(server.installSnapshot(r.getInstallSnapshot()));
      } else if (r.isStartLeaderElection()) {
        return new RaftServerReply(server.startLeaderElection(r.getStartLeaderElection()));
      } else {
        throw new IllegalStateException("unexpected state");
      }
    }
  };

  final RequestHandler.HandlerInterface<RaftClientRequest, RaftClientReply> clientHandlerImpl
      = new RequestHandler.HandlerInterface<RaftClientRequest, RaftClientReply>() {
    @Override
    public boolean isAlive() {
      return !server.getLifeCycleState().isClosingOrClosed();
    }

    @Override
    public RaftClientReply handleRequest(RaftClientRequest request)
        throws IOException {
      final CompletableFuture<RaftClientReply> future;
      if (request instanceof GroupManagementRequest) {
        future = CompletableFuture.completedFuture(
            server.groupManagement((GroupManagementRequest) request));
      } else if (request instanceof GroupListRequest) {
        future = CompletableFuture.completedFuture(
            server.getGroupList((GroupListRequest) request));
      } else if (request instanceof GroupInfoRequest) {
        future = CompletableFuture.completedFuture(
            server.getGroupInfo((GroupInfoRequest) request));
      } else if (request instanceof SetConfigurationRequest) {
        future = server.setConfigurationAsync((SetConfigurationRequest) request);
      } else if (request instanceof TransferLeadershipRequest) {
        future = server.transferLeadershipAsync((TransferLeadershipRequest) request);
      } else if (request instanceof SnapshotManagementRequest) {
        future = server.snapshotManagementAsync((SnapshotManagementRequest) request);
      } else if (request instanceof LeaderElectionManagementRequest) {
        future = server.leaderElectionManagementAsync((LeaderElectionManagementRequest) request);
      } else {
        future = server.submitClientRequestAsync(request);
      }

      future.whenCompleteAsync((reply, exception) -> {
        try {
          final IOException e = exception == null? null
              : IOUtils.asIOException(JavaUtils.unwrapCompletionException(exception));
          clientHandler.getRpc().sendReply(request, reply, e);
        } catch (IOException e) {
          LOG.warn("Failed to send reply {} for request {} due to exception {}",
              reply, request, e);
        }
      }, executor);
      return null;
    }
  };
}
