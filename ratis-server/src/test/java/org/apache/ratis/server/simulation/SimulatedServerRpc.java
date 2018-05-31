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
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerRpc;
import org.apache.ratis.server.impl.RaftServerProxy;
import org.apache.ratis.shaded.proto.RaftProtos.*;
import org.apache.ratis.util.Daemon;
import org.apache.ratis.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

class SimulatedServerRpc implements RaftServerRpc {
  static final Logger LOG = LoggerFactory.getLogger(SimulatedServerRpc.class);

  private final RaftServerProxy server;
  private final RequestHandler<RaftServerRequest, RaftServerReply> serverHandler;
  private final RequestHandler<RaftClientRequest, RaftClientReply> clientHandler;
  private final ExecutorService executor = Executors.newFixedThreadPool(3, Daemon::new);

  SimulatedServerRpc(RaftServer server,
      SimulatedRequestReply<RaftServerRequest, RaftServerReply> serverRequestReply,
      SimulatedRequestReply<RaftClientRequest, RaftClientReply> clientRequestReply) {
    this.server = (RaftServerProxy)server;

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
  public void addPeers(Iterable<RaftPeer> peers) {
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
      return RaftTestUtil.getImplAsUnchecked(server).isAlive();
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
      } else {
        throw new IllegalStateException("unexpected state");
      }
    }
  };

  final RequestHandler.HandlerInterface<RaftClientRequest, RaftClientReply> clientHandlerImpl
      = new RequestHandler.HandlerInterface<RaftClientRequest, RaftClientReply>() {
    @Override
    public boolean isAlive() {
      return RaftTestUtil.getImplAsUnchecked(server).isAlive();
    }

    @Override
    public RaftClientReply handleRequest(RaftClientRequest request)
        throws IOException {
      final CompletableFuture<RaftClientReply> future;
      if (request instanceof ReinitializeRequest) {
        future = CompletableFuture.completedFuture(
            server.reinitialize((ReinitializeRequest) request));
      } else if (request instanceof ServerInformationRequest) {
        future = CompletableFuture.completedFuture(
            server.getInfo((ServerInformationRequest) request));
      } else if (request instanceof SetConfigurationRequest) {
        future = server.setConfigurationAsync((SetConfigurationRequest) request);
      } else {
        future = server.submitClientRequestAsync(request);
      }

      future.whenCompleteAsync((reply, exception) -> {
        try {
          final IOException e = IOUtils.asIOException(exception);
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
