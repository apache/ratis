/**
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

import org.apache.ratis.protocol.*;
import org.apache.ratis.server.impl.RetryCache.CacheEntry;
import org.apache.ratis.proto.RaftProtos.ReplicationLevel;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.util.Preconditions;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class PendingRequest implements Comparable<PendingRequest> {
  private static class DelayedReply {
    private final RaftClientReply reply;
    private final CacheEntry cacheEntry;

    DelayedReply(RaftClientReply reply, CacheEntry cacheEntry) {
      this.reply = reply;
      this.cacheEntry = cacheEntry;
    }

    RaftClientReply getReply() {
      cacheEntry.updateResult(reply);
      return reply;
    }

    RaftClientReply fail(NotReplicatedException e) {
      final RaftClientReply failed = new RaftClientReply(reply, e);
      cacheEntry.updateResult(failed);
      return failed;
    }
  }

  private final long index;
  private final RaftClientRequest request;
  private final TransactionContext entry;
  private final CompletableFuture<RaftClientReply> future;

  private volatile DelayedReply delayed;

  PendingRequest(long index, RaftClientRequest request, TransactionContext entry) {
    this.index = index;
    this.request = request;
    this.entry = entry;
    this.future = new CompletableFuture<>();
  }

  PendingRequest(SetConfigurationRequest request) {
    this(RaftServerConstants.INVALID_LOG_INDEX, request, null);
  }

  long getIndex() {
    return index;
  }

  RaftClientRequest getRequest() {
    return request;
  }

  public CompletableFuture<RaftClientReply> getFuture() {
    return future;
  }

  TransactionContext getEntry() {
    return entry;
  }

  /**
   * This is only used when setting new raft configuration.
   */
  synchronized void setException(Throwable e) {
    Preconditions.assertTrue(e != null);
    future.completeExceptionally(e);
  }

  synchronized void setReply(RaftClientReply r) {
    Preconditions.assertTrue(r != null);
    future.complete(r);
  }

  synchronized void setDelayedReply(RaftClientReply r, CacheEntry c) {
    Objects.requireNonNull(r);
    Preconditions.assertTrue(delayed == null);
    delayed = new DelayedReply(r, c);
  }

  synchronized void completeDelayedReply() {
    setReply(delayed.getReply());
  }

  synchronized void failDelayedReply() {
    final ReplicationLevel replication = request.getType().getWrite().getReplication();
    final NotReplicatedException e = new NotReplicatedException(request.getCallId(), replication, index);
    setReply(delayed.fail(e));
  }

  TransactionContext setNotLeaderException(NotLeaderException nle) {
    setReply(new RaftClientReply(getRequest(), nle, null));
    return getEntry();
  }

  @Override
  public int compareTo(PendingRequest that) {
    return Long.compare(this.index, that.index);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(index=" + index
        + ", request=" + request;
  }
}
