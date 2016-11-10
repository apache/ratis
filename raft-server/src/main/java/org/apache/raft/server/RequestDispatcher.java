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

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import org.apache.raft.proto.RaftProtos;
import org.apache.raft.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.raft.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.raft.proto.RaftProtos.InstallSnapshotReplyProto;
import org.apache.raft.proto.RaftProtos.InstallSnapshotRequestProto;
import org.apache.raft.proto.RaftProtos.RequestVoteRequestProto;
import org.apache.raft.protocol.RaftClientReply;
import org.apache.raft.protocol.RaftClientRequest;
import org.apache.raft.protocol.SetConfigurationRequest;
import org.apache.raft.statemachine.StateMachine;
import org.apache.raft.statemachine.TrxContext;

/**
 * Each RPC request is first handled by the RequestDispatcher:
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
      // TODO: We might not be the leader anymore by the time this completes. See the RAFT paper,
      // section 8 (last part)
      return stateMachine.query(request);
    }

    // TODO: this client request will not be added to pending requests
    // until later which means that any failure in between will leave partial state in the
    // state machine. We should call cancelTransaction() for failed requests
    TrxContext entry = stateMachine.startTransaction(request);
    if (entry.getException().isPresent()) {
      Exception ex = entry.getException().get();
      throw ex instanceof IOException ? (IOException)ex : new IOException(ex);
    }

    return server.appendTransaction(request, entry);
  }

  public CompletableFuture<RaftClientReply> setConfiguration(
      SetConfigurationRequest request) throws IOException {
    return server.setConfiguration(request);
  }

  public RaftProtos.RequestVoteReplyProto requestVote(
      RequestVoteRequestProto request) throws IOException {
    return server.requestVote(request);
  }

  public AppendEntriesReplyProto appendEntries(AppendEntriesRequestProto request)
      throws IOException {
    return server.appendEntries(request);
  }

  public InstallSnapshotReplyProto installSnapshot(
      InstallSnapshotRequestProto request) throws IOException {
    return server.installSnapshot(request);
  }
}
