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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * Opportunistically batch follower-to-leader ReadIndex requests.
 *
 * <p>The batch is drained on the server executor without waiting for a timer. {@code batchSize}
 * is only a maximum drain cap, not a target size.
 */
class ReadIndexBatching {
  private final Executor executor;
  private final int batchSize;
  private final Function<RaftClientRequest, CompletableFuture<ReadIndexReplyProto>> readIndexAsyncImpl;

  /** Guarded by {@code this}. */
  private final Queue<Pending> pending = new ArrayDeque<>();
  /** Guarded by {@code this}. */
  private final HashSet<Batch> inFlight = new HashSet<>();
  /** Guarded by {@code this}; at most one drain task is scheduled or running. */
  private boolean drainScheduled;
  /** Guarded by {@code this}. */
  private boolean closed;

  ReadIndexBatching(Executor executor, int batchSize,
      Function<RaftClientRequest, CompletableFuture<ReadIndexReplyProto>> readIndexAsyncImpl) {
    this.executor = executor;
    this.batchSize = batchSize;
    this.readIndexAsyncImpl = readIndexAsyncImpl;
  }

  CompletableFuture<ReadIndexReplyProto> submit(RaftClientRequest request) {
    final CompletableFuture<ReadIndexReplyProto> future = new CompletableFuture<>();
    final boolean schedule;
    synchronized (this) {
      if (closed) {
        return JavaUtils.completeExceptionally(newClosedException());
      }
      pending.add(new Pending(request, future));
      schedule = !drainScheduled;
      if (schedule) {
        drainScheduled = true;
      }
    }

    if (schedule) {
      scheduleDrain();
    }
    return future;
  }

  void close() {
    close(newClosedException());
  }

  private void close(Throwable throwable) {
    final List<Pending> queued;
    final List<Batch> running;
    synchronized (this) {
      if (closed) {
        return;
      }
      closed = true;
      drainScheduled = false;
      queued = new ArrayList<>(pending);
      pending.clear();
      running = new ArrayList<>(inFlight);
      inFlight.clear();
    }
    queued.forEach(p -> p.future.completeExceptionally(throwable));
    running.forEach(batch -> batch.completeExceptionally(throwable));
  }

  private void scheduleDrain() {
    try {
      executor.execute(this::drain);
    } catch (RejectedExecutionException e) {
      close(new ReadIndexException("Failed to schedule ReadIndex batch drain.", e));
    }
  }

  private static ReadIndexException newClosedException() {
    return new ReadIndexException("ReadIndex batching is closed.");
  }

  private void drain() {
    final Batch batch;
    synchronized (this) {
      if (closed || pending.isEmpty()) {
        drainScheduled = false;
        return;
      }
      batch = pollBatch();
      inFlight.add(batch);
    }

    batch.send(readIndexAsyncImpl, () -> onBatchDone(batch));

    final boolean scheduleNext;
    synchronized (this) {
      if (closed) {
        scheduleNext = false;
      } else if (pending.isEmpty()) {
        drainScheduled = false;
        scheduleNext = false;
      } else {
        scheduleNext = true;
      }
    }
    if (scheduleNext) {
      scheduleDrain();
    }
  }

  private Batch pollBatch() {
    final List<Pending> batch = new ArrayList<>(Math.min(batchSize, pending.size()));
    for (int i = 0; i < batchSize; i++) {
      final Pending next = pending.poll();
      if (next == null) {
        break;
      }
      batch.add(next);
    }
    return new Batch(batch);
  }

  private void onBatchDone(Batch batch) {
    synchronized (this) {
      inFlight.remove(batch);
    }
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
    private final AtomicBoolean completed = new AtomicBoolean();
    private final List<Pending> pending;

    Batch(List<Pending> pending) {
      this.pending = pending;
    }

    void send(Function<RaftClientRequest, CompletableFuture<ReadIndexReplyProto>> readIndexAsyncImpl,
        Runnable onComplete) {
      if (pending.isEmpty()) {
        return;
      }
      if (completed.get()) {
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
        onComplete.run();
        return;
      }

      replyFuture.whenComplete((reply, throwable) -> {
        try {
          if (throwable != null) {
            completeExceptionally(JavaUtils.unwrapCompletionException(throwable));
          } else {
            complete(reply);
          }
        } finally {
          onComplete.run();
        }
      });
    }

    private void complete(ReadIndexReplyProto reply) {
      if (completed.compareAndSet(false, true)) {
        pending.forEach(p -> p.future.complete(reply));
      }
    }

    private void completeExceptionally(Throwable throwable) {
      if (completed.compareAndSet(false, true)) {
        pending.forEach(p -> p.future.completeExceptionally(throwable));
      }
    }
  }
}
