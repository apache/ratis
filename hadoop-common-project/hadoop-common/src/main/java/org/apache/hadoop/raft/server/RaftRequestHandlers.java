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
package org.apache.hadoop.raft.server;

import org.apache.hadoop.raft.protocol.RaftClientReply;
import org.apache.hadoop.raft.protocol.RaftClientRequest;
import org.apache.hadoop.raft.protocol.SetConfigurationRequest;
import org.apache.hadoop.raft.server.protocol.AppendEntriesRequest;
import org.apache.hadoop.raft.server.protocol.RaftServerReply;
import org.apache.hadoop.raft.server.protocol.RaftServerRequest;
import org.apache.hadoop.raft.server.protocol.RequestVoteRequest;

import java.io.IOException;

public class RaftRequestHandlers {
  private final RaftServer server;
  private final RequestHandler<RaftServerRequest, RaftServerReply> serverHandler;
  private final RequestHandler<RaftClientRequest, RaftClientReply> clientHandler;

  RaftRequestHandlers(RaftServer server,
                      RaftRpc<RaftServerRequest, RaftServerReply> serverRpc,
                      RaftRpc<RaftClientRequest, RaftClientReply> clientRpc) {
    this.server = server;
    this.serverHandler = new RequestHandler<>(server.getId(), "serverHandler",
        serverRpc, serverHandlerImpl);
    this.clientHandler = new RequestHandler<>(server.getId(), "clientHandler",
        clientRpc, clientHandlerImpl);
  }

  void start() {
    serverHandler.startDaemon();
    clientHandler.startDaemon();
  }

  void interruptAndJoinDaemon() throws InterruptedException {
    clientHandler.interruptAndJoinDaemon();
    serverHandler.interruptAndJoinDaemon();
  }

  void shutdown() throws IOException {
    clientHandler.shutdown();
    serverHandler.shutdown();
  }

  RaftServerReply sendServerRequest(RaftServerRequest request) throws IOException {
    return serverHandler.getRpc().sendRequest(request);
  }

  void sendClientReply(RaftClientRequest request, RaftClientReply reply, IOException ioe)
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
        final AppendEntriesRequest ap = (AppendEntriesRequest) r;
        return server.appendEntries(ap.getRequestorId(), ap.getLeaderTerm(),
            ap.getPreviousLog(), ap.getLeaderCommit(), ap.isInitializing(),
            ap.getEntries());
      } else if (r instanceof RequestVoteRequest) {
        final RequestVoteRequest rr = (RequestVoteRequest) r;
        return server.requestVote(rr.getCandidateId(), rr.getCandidateTerm(),
            rr.getLastLogIndex());
      } else { // TODO support other requests later
        // should not come here now
        return new RaftServerReply(r.getRequestorId(), server.getId(),
            server.getState().getCurrentTerm(), false);
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
        server.submit(request);
      }
      return null;
    }
  };
}
