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
package org.apache.raft.server.impl;

import org.apache.raft.protocol.*;
import org.apache.raft.server.protocol.RaftServerProtocol;
import org.apache.raft.shaded.proto.RaftProtos.*;
import org.apache.raft.statemachine.StateMachine;
import org.apache.raft.statemachine.TransactionContext;
import org.apache.raft.util.RaftUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

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
public class RequestDispatcher implements RaftClientProtocol, RaftServerProtocol {
  static final Logger LOG = LoggerFactory.getLogger(RequestDispatcher.class);

  private final RaftServer server;
  private final StateMachine stateMachine;

  public RequestDispatcher(RaftServer server) {
    this.server = server;
    this.stateMachine = server.getStateMachine();
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
    TransactionContext entry = stateMachine.startTransaction(request);
    if (entry.getException().isPresent()) {
      throw RaftUtils.asIOException(entry.getException().get());
    }

    return server.appendTransaction(request, entry);
  }

  @Override
  public RaftClientReply submitClientRequest(RaftClientRequest request)
      throws IOException {
    return waitForReply(server.getId(), request, handleClientRequest(request));
  }

  public CompletableFuture<RaftClientReply> setConfigurationAsync(
      SetConfigurationRequest request) throws IOException {
    return server.setConfiguration(request);
  }

  @Override
  public RaftClientReply setConfiguration(SetConfigurationRequest request)
      throws IOException {
    return waitForReply(server.getId(), request, setConfigurationAsync(request));
  }

  private static RaftClientReply waitForReply(String serverId,
      RaftClientRequest request, CompletableFuture<RaftClientReply> future)
      throws IOException {
    try {
      return future.get();
    } catch (InterruptedException e) {
      final String s = serverId + ": Interrupted when waiting for reply, request=" + request;
      LOG.info(s, e);
      throw RaftUtils.toInterruptedIOException(s, e);
    } catch (ExecutionException e) {
      final Throwable cause = e.getCause();
      if (cause == null) {
        throw new IOException(e);
      }
      if (cause instanceof NotLeaderException) {
        return new RaftClientReply(request, (NotLeaderException)cause);
      } else {
        throw RaftUtils.asIOException(cause);
      }
    }
  }

  @Override
  public RequestVoteReplyProto requestVote(RequestVoteRequestProto request)
      throws IOException {
    return server.requestVote(request);
  }

  @Override
  public AppendEntriesReplyProto appendEntries(AppendEntriesRequestProto request)
      throws IOException {
    return server.appendEntries(request);
  }

  @Override
  public InstallSnapshotReplyProto installSnapshot(
      InstallSnapshotRequestProto request) throws IOException {
    return server.installSnapshot(request);
  }
}
