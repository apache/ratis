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
package org.apache.raft.server.simulation;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.raft.protocol.RaftClientReply;
import org.apache.raft.protocol.RaftClientRequest;
import org.apache.raft.protocol.RaftPeer;
import org.apache.raft.protocol.SetConfigurationRequest;
import org.apache.raft.server.RaftServer;
import org.apache.raft.server.RaftServerRpc;
import org.apache.raft.server.RequestDispatcher;
import org.apache.raft.shaded.proto.RaftProtos.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class SimulatedServerRpc implements RaftServerRpc {
  static final Logger LOG = LoggerFactory.getLogger(SimulatedServerRpc.class);

  private final RaftServer server;
  private final RequestDispatcher dispatcher;
  private final RequestHandler<RaftServerRequest, RaftServerReply> serverHandler;
  private final RequestHandler<RaftClientRequest, RaftClientReply> clientHandler;
  private final ExecutorService executor = Executors.newFixedThreadPool(3,
      new ThreadFactoryBuilder().setDaemon(true).build());

  public SimulatedServerRpc(RaftServer server,
      SimulatedRequestReply<RaftServerRequest, RaftServerReply> serverRequestReply,
      SimulatedRequestReply<RaftClientRequest, RaftClientReply> clientRequestReply) {
    this.server = server;
    this.dispatcher = new RequestDispatcher(server);
    this.serverHandler = new RequestHandler<>(server.getId(),
        "serverHandler", serverRequestReply, serverHandlerImpl, 3);
    this.clientHandler = new RequestHandler<>(server.getId(),
        "clientHandler", clientRequestReply, clientHandlerImpl, 3);
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
  public void shutdown() {
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
  public AppendEntriesReplyProto sendAppendEntries(AppendEntriesRequestProto request)
      throws IOException {
    RaftServerReply reply = serverHandler.getRpc()
        .sendRequest(new RaftServerRequest(request));
    return reply.getAppendEntries();
  }

  @Override
  public InstallSnapshotReplyProto sendInstallSnapshot(InstallSnapshotRequestProto request)
      throws IOException {
    RaftServerReply reply = serverHandler.getRpc()
        .sendRequest(new RaftServerRequest(request));
    return reply.getInstallSnapshot();
  }

  @Override
  public RequestVoteReplyProto sendRequestVote(RequestVoteRequestProto request)
      throws IOException {
    RaftServerReply reply = serverHandler.getRpc()
        .sendRequest(new RaftServerRequest(request));
    return reply.getRequestVote();
  }

  @Override
  public void addPeers(Iterable<RaftPeer> peers) {
    // do nothing
  }

  final RequestHandler.HandlerInterface<RaftServerRequest, RaftServerReply> serverHandlerImpl
      = new RequestHandler.HandlerInterface<RaftServerRequest, RaftServerReply>() {
    @Override
    public boolean isAlive() {
      return server.isAlive();
    }

    @Override
    public RaftServerReply handleRequest(RaftServerRequest r)
        throws IOException {
      if (r.isAppendEntries()) {
        return new RaftServerReply(
            dispatcher.appendEntries(r.getAppendEntries()));
      } else if (r.isRequestVote()) {
        return new RaftServerReply(dispatcher.requestVote(r.getRequestVote()));
      } else if (r.isInstallSnapshot()) {
        return new RaftServerReply(
            dispatcher.installSnapshot(r.getInstallSnapshot()));
      } else {
        throw new IllegalStateException("unexpected state");
      }
    }
  };

  final RequestHandler.HandlerInterface<RaftClientRequest, RaftClientReply> clientHandlerImpl
      = new RequestHandler.HandlerInterface<RaftClientRequest, RaftClientReply>() {
    @Override
    public boolean isAlive() {
      return server.isAlive();
    }

    @Override
    public RaftClientReply handleRequest(RaftClientRequest request)
        throws IOException {
      final CompletableFuture<RaftClientReply> future;
      if (request instanceof SetConfigurationRequest) {
        future = dispatcher.setConfigurationAsync((SetConfigurationRequest) request);
      } else {
        future = dispatcher.handleClientRequest(request);
      }

      future.whenCompleteAsync((reply, exception) -> {
        try {
          IOException e = null;
          if (exception != null) {
            e = exception instanceof IOException ?
                (IOException) exception : new IOException(exception);
          }
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
