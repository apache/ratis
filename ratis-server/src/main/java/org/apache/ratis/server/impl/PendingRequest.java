/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ratis.server.impl;

import org.apache.ratis.proto.RaftProtos.RaftClientRequestProto.TypeCase;
import org.apache.ratis.proto.RaftProtos.CommitInfoProto;
import org.apache.ratis.protocol.*;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Preconditions;

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

class PendingRequest {
  private final TermIndex termIndex;
  private final RaftClientRequest request;
  private final TransactionContext entry;
  private final CompletableFuture<RaftClientReply> futureToComplete = new CompletableFuture<>();
  private final CompletableFuture<RaftClientReply> futureToReturn;

  PendingRequest(RaftClientRequest request, TransactionContext entry) {
    this.termIndex = entry == null? null: TermIndex.valueOf(entry.getLogEntryUnsafe());
    this.request = request;
    this.entry = entry;
    if (request.is(TypeCase.FORWARD)) {
      futureToReturn = futureToComplete.thenApply(reply -> convert(request, reply));
    } else {
      futureToReturn = futureToComplete;
    }
  }

  PendingRequest(SetConfigurationRequest request) {
    this(request, null);
  }

  RaftClientReply convert(RaftClientRequest q, RaftClientReply p) {
    return RaftClientReply.newBuilder()
        .setRequest(q)
        .setCommitInfos(p.getCommitInfos())
        .setLogIndex(p.getLogIndex())
        .setMessage(p.getMessage())
        .setException(p.getException())
        .setSuccess(p.isSuccess())
        .build();
  }

  TermIndex getTermIndex() {
    return Objects.requireNonNull(termIndex, "termIndex");
  }

  RaftClientRequest getRequest() {
    return request;
  }

  public CompletableFuture<RaftClientReply> getFuture() {
    return futureToReturn;
  }

  TransactionContext getEntry() {
    return entry;
  }

  /**
   * This is only used when setting new raft configuration.
   */
  synchronized void setException(Throwable e) {
    Preconditions.assertTrue(e != null);
    futureToComplete.completeExceptionally(e);
  }

  synchronized void setReply(RaftClientReply r) {
    Preconditions.assertTrue(r != null);
    futureToComplete.complete(r);
  }

  TransactionContext setNotLeaderException(NotLeaderException nle, Collection<CommitInfoProto> commitInfos) {
    setReply(RaftClientReply.newBuilder()
        .setRequest(getRequest())
        .setException(nle)
        .setCommitInfos(commitInfos)
        .build());
    return getEntry();
  }

  @Override
  public String toString() {
    return JavaUtils.getClassSimpleName(getClass()) + "-" + termIndex + ":request=" + request;
  }
}
