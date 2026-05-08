/*
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
package org.apache.ratis.server.impl;

import org.apache.ratis.proto.RaftProtos.ReadIndexReplyProto;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.exceptions.ReadIndexException;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.TimeoutExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/** Batch follower-to-leader ReadIndex requests. */
class ReadIndexBatching {
  private static final Logger LOG = LoggerFactory.getLogger(ReadIndexBatching.class);

  private final TimeoutExecutor scheduler;
  private final TimeDuration batchInterval;
  private final int batchSize;
  private final Function<RaftClientRequest, CompletableFuture<ReadIndexReplyProto>> readIndexAsyncImpl;

  /** Guarded by {@code this}; the monitor provides visibility, so volatile is not needed. */
  private Batch open;
  /** Guarded by {@code this}. */
  private boolean closed;

  ReadIndexBatching(TimeDuration batchInterval, int batchSize,
      Function<RaftClientRequest, CompletableFuture<ReadIndexReplyProto>> readIndexAsyncImpl) {
    this(TimeoutExecutor.getInstance(), batchInterval, batchSize, readIndexAsyncImpl);
  }

  ReadIndexBatching(TimeoutExecutor scheduler, TimeDuration batchInterval, int batchSize,
      Function<RaftClientRequest, CompletableFuture<ReadIndexReplyProto>> readIndexAsyncImpl) {
    this.scheduler = scheduler;
    this.batchInterval = batchInterval;
    this.batchSize = batchSize;
    this.readIndexAsyncImpl = readIndexAsyncImpl;
  }

  CompletableFuture<ReadIndexReplyProto> submit(RaftClientRequest request) {
    final CompletableFuture<ReadIndexReplyProto> future = new CompletableFuture<>();
    final Batch batch;
    final boolean schedule;
    final boolean seal;
    synchronized (this) {
      if (closed) {
        return JavaUtils.completeExceptionally(newClosedException());
      }
      schedule = open == null;
      if (schedule) {
        open = new Batch();
      }
      batch = open;
      batch.add(request, future);
      seal = batch.size() >= batchSize;
      if (seal) {
        open = null;
      }
    }

    if (schedule) {
      scheduler.onTimeout(batchInterval, () -> seal(batch), LOG,
          () -> "Failed to seal ReadIndex batch");
    }
    if (seal) {
      batch.seal(readIndexAsyncImpl);
    }
    return future;
  }

  void close() {
    final Batch batch;
    synchronized (this) {
      closed = true;
      batch = open;
      open = null;
    }
    if (batch != null) {
      batch.cancel(newClosedException());
    }
  }

  private static ReadIndexException newClosedException() {
    return new ReadIndexException("ReadIndex batching is closed.");
  }

  private void seal(Batch batch) {
    synchronized (this) {
      if (open == batch) {
        open = null;
      }
    }
    batch.seal(readIndexAsyncImpl);
  }

  private static class Pending {
    private final RaftClientRequest request;
    private final CompletableFuture<ReadIndexReplyProto> future;

    Pending(RaftClientRequest request, CompletableFuture<ReadIndexReplyProto> future) {
      this.request = request;
      this.future = future;
    }
  }

  private static class Batch {
    private final AtomicBoolean sealed = new AtomicBoolean();
    private final List<Pending> pending = new ArrayList<>();

    void add(RaftClientRequest request, CompletableFuture<ReadIndexReplyProto> future) {
      pending.add(new Pending(request, future));
    }

    int size() {
      return pending.size();
    }

    void seal(Function<RaftClientRequest, CompletableFuture<ReadIndexReplyProto>> readIndexAsyncImpl) {
      if (!sealed.compareAndSet(false, true)) {
        return;
      }
      if (pending.isEmpty()) {
        return;
      }

      final CompletableFuture<ReadIndexReplyProto> replyFuture;
      try {
        // Plain reads only need one ReadIndex RPC for the batch.  Read-after-write requests
        // bypass batching before reaching this class, since their client request carries
        // per-client write-index state.
        replyFuture = readIndexAsyncImpl.apply(pending.get(0).request);
      } catch (Throwable t) {
        completeExceptionally(t);
        return;
      }

      replyFuture.whenComplete((reply, throwable) -> {
        if (throwable != null) {
          completeExceptionally(JavaUtils.unwrapCompletionException(throwable));
        } else {
          pending.forEach(p -> p.future.complete(reply));
        }
      });
    }

    private void cancel(Throwable throwable) {
      if (sealed.compareAndSet(false, true)) {
        completeExceptionally(throwable);
      }
    }

    private void completeExceptionally(Throwable throwable) {
      pending.forEach(p -> p.future.completeExceptionally(throwable));
    }
  }
}
