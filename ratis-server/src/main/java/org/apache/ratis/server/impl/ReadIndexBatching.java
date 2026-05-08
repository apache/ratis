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

  private final TimeoutExecutor scheduler = TimeoutExecutor.getInstance();
  private final TimeDuration batchInterval;
  private final int batchSize;
  private final Function<RaftClientRequest, CompletableFuture<ReadIndexReplyProto>> sender;

  private Batch open;

  ReadIndexBatching(TimeDuration batchInterval, int batchSize,
      Function<RaftClientRequest, CompletableFuture<ReadIndexReplyProto>> sender) {
    this.batchInterval = batchInterval;
    this.batchSize = batchSize;
    this.sender = sender;
  }

  CompletableFuture<ReadIndexReplyProto> submit(RaftClientRequest request) {
    final CompletableFuture<ReadIndexReplyProto> future = new CompletableFuture<>();
    final Batch batch;
    final boolean schedule;
    final boolean seal;
    synchronized (this) {
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
      batch.seal(sender);
    }
    return future;
  }

  private void seal(Batch batch) {
    synchronized (this) {
      if (open == batch) {
        open = null;
      }
    }
    batch.seal(sender);
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

    void seal(Function<RaftClientRequest, CompletableFuture<ReadIndexReplyProto>> sender) {
      if (!sealed.compareAndSet(false, true)) {
        return;
      }
      if (pending.isEmpty()) {
        return;
      }

      final CompletableFuture<ReadIndexReplyProto> replyFuture;
      try {
        replyFuture = sender.apply(pending.get(0).request);
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

    private void completeExceptionally(Throwable throwable) {
      pending.forEach(p -> p.future.completeExceptionally(throwable));
    }
  }
}
