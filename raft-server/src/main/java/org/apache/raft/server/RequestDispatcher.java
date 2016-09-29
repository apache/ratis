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
package org.apache.raft.server;

import org.apache.raft.protocol.RaftClientReply;
import org.apache.raft.protocol.RaftClientRequest;
import org.apache.raft.protocol.SetConfigurationRequest;
import org.apache.raft.server.StateMachine.ClientOperationEntry;
import org.apache.raft.server.protocol.AppendEntriesReply;
import org.apache.raft.server.protocol.AppendEntriesRequest;
import org.apache.raft.server.protocol.InstallSnapshotReply;
import org.apache.raft.server.protocol.InstallSnapshotRequest;
import org.apache.raft.server.protocol.RequestVoteReply;
import org.apache.raft.server.protocol.RequestVoteRequest;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * Each RPC request is first handled by the RequestDistributor:
 * 1. A request from another RaftPeer is to be handled by RaftServer.
 *
 * If the raft peer is the leader, then:
 *
 * 2. A read-only request from client is to be handled by the state machine.
 * 3. A write request from client is first validated by the state machine. The
 * state machine returns the content of the raft log entry, which is then passed
 * to the RaftServer for replication.
 */
public class RequestDispatcher {
  private final RaftServer server;
  private final StateMachine stateMachine;

  public RequestDispatcher(RaftServer server) {
    this.server = server;
    this.stateMachine = server.getStateMachine();
  }

  public RaftServer getRaftServer() {
    return server;
  }

  public CompletableFuture<RaftClientReply> handleClientRequest(
      RaftClientRequest request) throws IOException {
    // first check the server's leader state
    CompletableFuture<RaftClientReply> reply = server.checkLeaderState(request);
    if (reply != null) {
      return reply;
    }

    // let the state machine handle read-only request from client
    if (request.isReadOnly()) {
      return stateMachine.queryStateMachine(request);
    }

    ClientOperationEntry entry = stateMachine.validateUpdate(request);
    if (entry == null) {
      throw new IOException("The reqeust is rejected by the state machine");
    }

    return server.appendClientOperation(request, entry);
  }

  public CompletableFuture<RaftClientReply> setConfiguration(
      SetConfigurationRequest request) throws IOException {
    return server.setConfiguration(request);
  }

  public RequestVoteReply requestVote(RequestVoteRequest request)
      throws IOException {
    return server.requestVote(request);
  }

  public AppendEntriesReply appendEntries(AppendEntriesRequest request)
      throws IOException {
    return server.appendEntries(request);
  }

  public InstallSnapshotReply installSnapshot(InstallSnapshotRequest request)
      throws IOException {
    return server.installSnapshot(request);
  }
}
