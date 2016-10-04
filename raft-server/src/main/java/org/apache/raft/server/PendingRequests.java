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

import com.google.common.base.Preconditions;
import org.apache.raft.protocol.*;
import org.apache.raft.statemachine.TrxContext;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Deque;
import java.util.LinkedList;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

class PendingRequests {
  private static final Logger LOG = RaftServer.LOG;

  private PendingRequest pendingSetConf;
  private final RaftServer server;
  private final Deque<PendingRequest> pendingRequests = new LinkedList<>();

  PendingRequests(RaftServer server) {
    this.server = server;
  }

  PendingRequest addPendingRequest(long index, RaftClientRequest request,
      TrxContext entry) {
    Preconditions.checkArgument(!request.isReadOnly());
    final PendingRequest last = pendingRequests.peekLast();
    Preconditions.checkState(last == null || index == last.getIndex() + 1);
    return add(index, request, entry);
  }

  private PendingRequest add(long index, RaftClientRequest request,
      TrxContext entry) {
    final PendingRequest pending = new PendingRequest(index, request, entry);
    pendingRequests.offer(pending);
    return pending;
  }

  PendingRequest addConfRequest(SetConfigurationRequest request) {
    Preconditions.checkState(pendingSetConf == null);
    pendingSetConf = new PendingRequest(request);
    return pendingSetConf;
  }

  void replySetConfiguration() {
    // we allow the pendingRequest to be null in case that the new leader
    // commits the new configuration while it has not received the retry
    // request from the client
    if (pendingSetConf != null) {
      // for setConfiguration we do not need to wait for statemachine. send back
      // reply after it's committed.
      pendingSetConf.setSuccessReply(null);
      pendingSetConf = null;
    }
  }

  void failSetConfiguration(RaftException e) {
    Preconditions.checkState(pendingSetConf != null);
    pendingSetConf.setException(e);
    pendingSetConf = null;
  }

  void replyPendingRequest(long index, CompletableFuture<Message> messageFuture) {
    final PendingRequest pending = pendingRequests.poll();
    if (pending != null) {
      Preconditions.checkState(pending.getIndex() == index);

      messageFuture.whenCompleteAsync((reply, exception) -> {
        if (exception == null) {
          pending.setSuccessReply(reply);
        } else {
          pending.setException(exception);
        }
      });
    }
  }

  /**
   * The leader state is stopped. Send NotLeaderException to all the pending
   * requests since they have not got applied to the state machine yet.
   */
  void sendNotLeaderResponses() {
    LOG.info("{} sends responses before shutting down PendingRequestsHandler",
        server.getId());

    Collection<TrxContext> pendingEntries = pendingRequests.stream()
        .map(PendingRequest::getEntry).collect(Collectors.toList());
    // notify the state machine about stepping down
    server.getStateMachine().notifyNotLeader(pendingEntries);
    pendingRequests.forEach(this::setNotLeaderException);
    if (pendingSetConf != null) {
      setNotLeaderException(pendingSetConf);
    }
  }

  private void setNotLeaderException(PendingRequest pending) {
    RaftClientReply reply = new RaftClientReply(pending.getRequest(),
        server.generateNotLeaderException());
    pending.setReply(reply);
  }
}
