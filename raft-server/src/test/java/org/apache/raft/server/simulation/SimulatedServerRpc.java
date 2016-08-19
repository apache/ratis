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
import org.apache.raft.protocol.RaftException;
import org.apache.raft.protocol.RaftPeer;
import org.apache.raft.protocol.SetConfigurationRequest;
import org.apache.raft.server.RaftServer;
import org.apache.raft.server.RaftServerRpc;
import org.apache.raft.server.protocol.AppendEntriesRequest;
import org.apache.raft.server.protocol.InstallSnapshotRequest;
import org.apache.raft.server.protocol.RaftServerReply;
import org.apache.raft.server.protocol.RaftServerRequest;
import org.apache.raft.server.protocol.RequestVoteRequest;
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
  private final RequestHandler<RaftServerRequest, RaftServerReply> serverHandler;
  private final RequestHandler<RaftClientRequest, RaftClientReply> clientHandler;
  private final ExecutorService executor = Executors.newFixedThreadPool(3,
      new ThreadFactoryBuilder().setDaemon(true).build());

  public SimulatedServerRpc(RaftServer server,
      SimulatedRequestReply<RaftServerRequest, RaftServerReply> serverRequestReply,
      SimulatedRequestReply<RaftClientRequest, RaftClientReply> clientRequestReply) {
    this.server = server;
    this.serverHandler = new RequestHandler<>(server.getId(), "serverHandler",
        serverRequestReply, serverHandlerImpl, 3);
    this.clientHandler = new RequestHandler<>(server.getId(), "clientHandler",
        clientRequestReply, clientHandlerImpl, 3);
  }

  @Override
  public void start() {
    serverHandler.startDaemon();
    clientHandler.startDaemon();
  }

  @Override
  public void interruptAndJoin() throws InterruptedException {
    clientHandler.interruptAndJoinDaemon();
    serverHandler.interruptAndJoinDaemon();
  }

  @Override
  public void shutdown() {
    try {
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
  public RaftServerReply sendServerRequest(RaftServerRequest request) throws IOException {
    return serverHandler.getRpc().sendRequest(request);
  }

  @Override
  public void addPeerProxies(Iterable<RaftPeer> peers) {
    // do nothing
  }

  final RequestHandler.HandlerInterface<RaftServerRequest, RaftServerReply> serverHandlerImpl
      = new RequestHandler.HandlerInterface<RaftServerRequest, RaftServerReply>() {
    @Override
    public boolean isRunning() {
      return server.isRunning();
    }

    @Override
    public RaftServerReply handleRequest(RaftServerRequest r)
        throws IOException {
      if (r instanceof AppendEntriesRequest) {
        return server.appendEntries((AppendEntriesRequest) r);
      } else if (r instanceof RequestVoteRequest) {
        return server.requestVote((RequestVoteRequest) r);
      } else if (r instanceof InstallSnapshotRequest) {
        return server.installSnapshot((InstallSnapshotRequest) r);
      } else {
        throw new IllegalStateException("unexpected state");
      }
    }
  };

  final RequestHandler.HandlerInterface<RaftClientRequest, RaftClientReply> clientHandlerImpl
      = new RequestHandler.HandlerInterface<RaftClientRequest, RaftClientReply>() {
    @Override
    public boolean isRunning() {
      return server.isRunning();
    }

    @Override
    public RaftClientReply handleRequest(RaftClientRequest request)
        throws IOException {
      final CompletableFuture<RaftClientReply> future;
      if (request instanceof SetConfigurationRequest) {
        future = server.setConfiguration((SetConfigurationRequest) request);
      } else {
        future = server.submitClientRequest(request);
      }

      future.handleAsync((reply, exception) -> {
        try {
          RaftException e = null;
          if (exception != null) {
            e = exception instanceof RaftException ?
                (RaftException) exception : new RaftException(exception);
          }
          clientHandler.getRpc().sendReply(request, reply, e);
        } catch (IOException e) {
          LOG.warn("Failed to send reply {} for request {} due to exception {}",
              reply, request, e);
        }
        return reply;
      }, executor);
      return null;
    }
  };
}
