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

import org.apache.hadoop.raft.protocol.RaftClientReply;
import org.apache.hadoop.raft.protocol.RaftClientRequest;
import org.apache.hadoop.raft.protocol.SetConfigurationRequest;
import org.apache.hadoop.raft.server.RaftServerRpc;
import org.apache.hadoop.raft.server.RaftServer;
import org.apache.hadoop.raft.server.protocol.*;

import java.io.IOException;

public class SimulatedServerRpc implements RaftServerRpc {
  private final RaftServer server;
  private final RequestHandler<RaftServerRequest, RaftServerReply> serverHandler;
  private final RequestHandler<RaftClientRequest, RaftClientReply> clientHandler;

  public SimulatedServerRpc(RaftServer server,
                            RequestReply<RaftServerRequest, RaftServerReply> serverRequestReply,
                            RequestReply<RaftClientRequest, RaftClientReply> clientRequestReply) {
    this.server = server;
    this.serverHandler = new RequestHandler<>(server.getId(), "serverHandler",
        serverRequestReply, serverHandlerImpl);
    this.clientHandler = new RequestHandler<>(server.getId(), "clientHandler",
        clientRequestReply, clientHandlerImpl);
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
    clientHandler.shutdown();
    serverHandler.shutdown();
  }

  @Override
  public RaftServerReply sendServerRequest(RaftServerRequest request) throws IOException {
    return serverHandler.getRpc().sendRequest(request);
  }

  @Override
  public void sendClientReply(RaftClientRequest request, RaftClientReply reply, IOException ioe)
      throws IOException {
    clientHandler.getRpc().sendReply(request, reply, ioe);
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
      } else { // TODO support other requests later
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
      if (request instanceof SetConfigurationRequest) {
        server.setConfiguration((SetConfigurationRequest) request);
      } else {
        server.submitClientRequest(request);
      }
      // Client reply is asynchronous since it needs to wait for log commit.
      return null;
    }
  };
}
