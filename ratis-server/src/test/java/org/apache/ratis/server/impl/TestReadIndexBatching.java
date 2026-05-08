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

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.ReadIndexReplyProto;
import org.apache.ratis.protocol.exceptions.ReadIndexException;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.TimeoutExecutor;
import org.apache.ratis.util.function.CheckedRunnable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

class TestReadIndexBatching {
  @Test
  void testBatchIntervalMustBePositive() {
    final RaftProperties properties = new RaftProperties();
    final TimeDuration zero = TimeDuration.valueOf(0, TimeUnit.MICROSECONDS);

    Assertions.assertThrows(IllegalArgumentException.class,
        () -> RaftServerConfigKeys.Read.ReadIndex.Batch.setBatchInterval(properties, zero));

    properties.setTimeDuration(RaftServerConfigKeys.Read.ReadIndex.Batch.BATCH_INTERVAL_KEY, zero);
    Assertions.assertThrows(IllegalArgumentException.class,
        () -> RaftServerConfigKeys.Read.ReadIndex.Batch.batchInterval(properties));
  }

  @Test
  void testBatchSizeSealsImmediately() throws Exception {
    final CapturingTimeoutExecutor scheduler = new CapturingTimeoutExecutor();
    final AtomicInteger readIndexCount = new AtomicInteger();
    final ReadIndexReplyProto readIndexReply = ReadIndexReplyProto.getDefaultInstance();
    final ReadIndexBatching batching = new ReadIndexBatching(
        scheduler, TimeDuration.valueOf(1, TimeUnit.DAYS), 2, request -> {
          readIndexCount.incrementAndGet();
          return CompletableFuture.completedFuture(readIndexReply);
        });

    final CompletableFuture<ReadIndexReplyProto> first = batching.submit(null);
    Assertions.assertFalse(first.isDone());
    Assertions.assertEquals(1, scheduler.getTaskCount());

    final CompletableFuture<ReadIndexReplyProto> second = batching.submit(null);
    Assertions.assertEquals(1, readIndexCount.get());
    Assertions.assertSame(readIndexReply, first.get());
    Assertions.assertSame(readIndexReply, second.get());

    scheduler.runNext();
    Assertions.assertEquals(1, readIndexCount.get());
  }

  @Test
  void testBatchIntervalSealsOpenBatch() throws Exception {
    final CapturingTimeoutExecutor scheduler = new CapturingTimeoutExecutor();
    final AtomicInteger readIndexCount = new AtomicInteger();
    final ReadIndexReplyProto readIndexReply = ReadIndexReplyProto.getDefaultInstance();
    final ReadIndexBatching batching = new ReadIndexBatching(
        scheduler, TimeDuration.valueOf(1, TimeUnit.DAYS), 64, request -> {
          readIndexCount.incrementAndGet();
          return CompletableFuture.completedFuture(readIndexReply);
        });

    final CompletableFuture<ReadIndexReplyProto> reply = batching.submit(null);
    Assertions.assertFalse(reply.isDone());
    Assertions.assertEquals(1, scheduler.getTaskCount());

    scheduler.runNext();
    Assertions.assertEquals(1, readIndexCount.get());
    Assertions.assertSame(readIndexReply, reply.get());
  }

  @Test
  void testReadIndexFailureCompletesAllBatchFuturesExceptionally() throws Exception {
    final RuntimeException failure = new RuntimeException("read index failed");
    final CompletableFuture<ReadIndexReplyProto> failed = new CompletableFuture<>();
    failed.completeExceptionally(failure);
    final ReadIndexBatching batching = new ReadIndexBatching(
        new NoOpTimeoutExecutor(), TimeDuration.valueOf(1, TimeUnit.DAYS), 2, request -> failed);

    final CompletableFuture<ReadIndexReplyProto> first = batching.submit(null);
    final CompletableFuture<ReadIndexReplyProto> second = batching.submit(null);

    Assertions.assertSame(failure,
        Assertions.assertThrows(ExecutionException.class, first::get).getCause());
    Assertions.assertSame(failure,
        Assertions.assertThrows(ExecutionException.class, second::get).getCause());
  }

  @Test
  void testCloseCompletesOpenBatchExceptionally() throws Exception {
    final AtomicInteger readIndexCount = new AtomicInteger();
    final ReadIndexBatching batching = new ReadIndexBatching(
        new NoOpTimeoutExecutor(), TimeDuration.valueOf(1, TimeUnit.DAYS), 64, request -> {
          readIndexCount.incrementAndGet();
          return new CompletableFuture<ReadIndexReplyProto>();
        });

    final CompletableFuture<ReadIndexReplyProto> reply = batching.submit(null);
    Assertions.assertFalse(reply.isDone());

    batching.close();

    final ExecutionException e = Assertions.assertThrows(ExecutionException.class, reply::get);
    Assertions.assertTrue(e.getCause() instanceof ReadIndexException);
    Assertions.assertEquals(0, readIndexCount.get());
  }

  @Test
  void testSubmitAfterCloseCompletesExceptionally() {
    final AtomicInteger readIndexCount = new AtomicInteger();
    final ReadIndexBatching batching = new ReadIndexBatching(
        new NoOpTimeoutExecutor(), TimeDuration.valueOf(1, TimeUnit.DAYS), 64, request -> {
          readIndexCount.incrementAndGet();
          return new CompletableFuture<ReadIndexReplyProto>();
        });

    batching.close();

    final CompletableFuture<ReadIndexReplyProto> reply = batching.submit(null);
    final ExecutionException e = Assertions.assertThrows(ExecutionException.class, reply::get);
    Assertions.assertTrue(e.getCause() instanceof ReadIndexException);
    Assertions.assertEquals(0, readIndexCount.get());
  }

  private static class NoOpTimeoutExecutor extends CapturingTimeoutExecutor {
    @Override
    public <THROWABLE extends Throwable> void onTimeout(
        TimeDuration timeout, CheckedRunnable<THROWABLE> task, Consumer<THROWABLE> errorHandler) {
    }

    @Override
    void runNext() {
    }
  }

  private static class CapturingTimeoutExecutor implements TimeoutExecutor {
    private final List<CheckedRunnable<?>> tasks = new ArrayList<>();

    @Override
    public int getTaskCount() {
      return tasks.size();
    }

    @Override
    public <THROWABLE extends Throwable> void onTimeout(
        TimeDuration timeout, CheckedRunnable<THROWABLE> task, Consumer<THROWABLE> errorHandler) {
      tasks.add(task);
    }

    void runNext() throws Exception {
      final CheckedRunnable<?> task = tasks.remove(0);
      try {
        task.run();
      } catch (RuntimeException | Error e) {
        throw e;
      } catch (Throwable t) {
        throw new AssertionError(t);
      }
    }
  }
}
