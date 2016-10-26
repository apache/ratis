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

import org.apache.raft.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.raft.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.raft.proto.RaftProtos.InstallSnapshotReplyProto;
import org.apache.raft.proto.RaftProtos.InstallSnapshotRequestProto;
import org.apache.raft.proto.RaftProtos.RequestVoteReplyProto;
import org.apache.raft.proto.RaftProtos.RequestVoteRequestProto;
import org.apache.raft.protocol.NotLeaderException;
import org.apache.raft.protocol.RaftClientProtocol;
import org.apache.raft.protocol.RaftClientReply;
import org.apache.raft.protocol.RaftClientRequest;
import org.apache.raft.protocol.SetConfigurationRequest;
import org.apache.raft.server.protocol.RaftServerProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class RaftServerRpcService implements RaftClientProtocol, RaftServerProtocol {
  static final Logger LOG = LoggerFactory.getLogger(RaftServerRpcService.class);
  private final RequestDispatcher dispatcher;

  public RaftServerRpcService(RequestDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  @Override
  public RaftClientReply submitClientRequest(RaftClientRequest request)
      throws IOException {
    CompletableFuture<RaftClientReply> future =
        dispatcher.handleClientRequest(request);
    return waitForReply(request, future);
  }

  @Override
  public RaftClientReply setConfiguration(SetConfigurationRequest request)
      throws IOException {
    CompletableFuture<RaftClientReply> future =
        dispatcher.setConfiguration(request);
    return waitForReply(request, future);
  }

  private RaftClientReply waitForReply(RaftClientRequest request,
      CompletableFuture<RaftClientReply> future) throws IOException {
    try {
      return future.get();
    } catch (InterruptedException e) {
      LOG.info("Interrupted when waiting for reply", e);
      throw new InterruptedIOException("Interrupted when waiting for reply");
    } catch (ExecutionException e) {
      final Throwable cause = e.getCause();
      if (cause instanceof NotLeaderException) {
        return new RaftClientReply(request, (NotLeaderException)cause);
      } else if (cause instanceof IOException) {
        throw (IOException) cause;
      } else {
        throw cause != null ? new IOException(cause) : new IOException(e);
      }
    }
  }

  @Override
  public RequestVoteReplyProto requestVote(RequestVoteRequestProto request)
      throws IOException {
    return dispatcher.requestVote(request);
  }

  @Override
  public AppendEntriesReplyProto appendEntries(AppendEntriesRequestProto request)
      throws IOException {
    return dispatcher.appendEntries(request);
  }

  @Override
  public InstallSnapshotReplyProto installSnapshot(
      InstallSnapshotRequestProto request) throws IOException {
    return dispatcher.installSnapshot(request);
  }

  public String getId() {
    return dispatcher.getRaftServer().getId();
  }
}
