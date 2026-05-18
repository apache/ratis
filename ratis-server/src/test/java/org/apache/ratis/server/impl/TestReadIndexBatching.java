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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

class TestReadIndexBatching {
  @Test
  void testBatchSizeMustBePositive() {
    final RaftProperties properties = new RaftProperties();

    Assertions.assertThrows(IllegalArgumentException.class,
        () -> RaftServerConfigKeys.Read.ReadIndex.Batch.setBatchSize(properties, 0));

    properties.setInt(RaftServerConfigKeys.Read.ReadIndex.Batch.BATCH_SIZE_KEY, 0);
    Assertions.assertThrows(IllegalArgumentException.class,
        () -> RaftServerConfigKeys.Read.ReadIndex.Batch.batchSize(properties));
  }

  @Test
  void testSubmitSchedulesOneOpportunisticDrain() throws Exception {
    final CapturingExecutor executor = new CapturingExecutor();
    final AtomicInteger readIndexCount = new AtomicInteger();
    final ReadIndexReplyProto readIndexReply = ReadIndexReplyProto.getDefaultInstance();
    final ReadIndexBatching batching = new ReadIndexBatching(
        executor, 64, request -> {
          readIndexCount.incrementAndGet();
          return CompletableFuture.completedFuture(readIndexReply);
        });

    final CompletableFuture<ReadIndexReplyProto> first = batching.submit(null);
    Assertions.assertFalse(first.isDone());
    Assertions.assertEquals(1, executor.getTaskCount());

    final CompletableFuture<ReadIndexReplyProto> second = batching.submit(null);
    Assertions.assertFalse(second.isDone());
    Assertions.assertEquals(1, executor.getTaskCount());

    executor.runNext();
    Assertions.assertEquals(1, readIndexCount.get());
    Assertions.assertSame(readIndexReply, first.get());
    Assertions.assertSame(readIndexReply, second.get());
  }

  @Test
  void testBatchSizeCapsEachDrain() throws Exception {
    final CapturingExecutor executor = new CapturingExecutor();
    final AtomicInteger readIndexCount = new AtomicInteger();
    final ReadIndexReplyProto readIndexReply = ReadIndexReplyProto.getDefaultInstance();
    final ReadIndexBatching batching = new ReadIndexBatching(
        executor, 2, request -> {
          readIndexCount.incrementAndGet();
          return CompletableFuture.completedFuture(readIndexReply);
        });

    final CompletableFuture<ReadIndexReplyProto> first = batching.submit(null);
    final CompletableFuture<ReadIndexReplyProto> second = batching.submit(null);
    final CompletableFuture<ReadIndexReplyProto> third = batching.submit(null);

    executor.runNext();
    Assertions.assertEquals(1, readIndexCount.get());
    Assertions.assertSame(readIndexReply, first.get());
    Assertions.assertSame(readIndexReply, second.get());
    Assertions.assertFalse(third.isDone());
    Assertions.assertEquals(1, executor.getTaskCount());

    executor.runNext();
    Assertions.assertEquals(2, readIndexCount.get());
    Assertions.assertSame(readIndexReply, third.get());
  }

  @Test
  void testReadIndexFailureCompletesBatchFuturesExceptionally() throws Exception {
    final CapturingExecutor executor = new CapturingExecutor();
    final RuntimeException failure = new RuntimeException("read index failed");
    final CompletableFuture<ReadIndexReplyProto> failed = new CompletableFuture<>();
    failed.completeExceptionally(failure);
    final ReadIndexBatching batching = new ReadIndexBatching(executor, 64, request -> failed);

    final CompletableFuture<ReadIndexReplyProto> first = batching.submit(null);
    final CompletableFuture<ReadIndexReplyProto> second = batching.submit(null);

    executor.runNext();
    Assertions.assertSame(failure,
        Assertions.assertThrows(ExecutionException.class, first::get).getCause());
    Assertions.assertSame(failure,
        Assertions.assertThrows(ExecutionException.class, second::get).getCause());
  }

  @Test
  void testCloseCompletesQueuedAndInFlightBatchesExceptionally() throws Exception {
    final CapturingExecutor executor = new CapturingExecutor();
    final AtomicInteger readIndexCount = new AtomicInteger();
    final CompletableFuture<ReadIndexReplyProto> readIndexFuture = new CompletableFuture<>();
    final ReadIndexBatching batching = new ReadIndexBatching(
        executor, 1, request -> {
          readIndexCount.incrementAndGet();
          return readIndexFuture;
        });

    final CompletableFuture<ReadIndexReplyProto> inFlight = batching.submit(null);
    final CompletableFuture<ReadIndexReplyProto> queued = batching.submit(null);
    executor.runNext();

    Assertions.assertEquals(1, readIndexCount.get());
    Assertions.assertFalse(inFlight.isDone());
    Assertions.assertFalse(queued.isDone());

    batching.close();

    assertReadIndexException(inFlight);
    assertReadIndexException(queued);

    readIndexFuture.complete(ReadIndexReplyProto.getDefaultInstance());
    assertReadIndexException(inFlight);
  }

  @Test
  void testScheduleFailureClosesBatching() throws Exception {
    final ReadIndexBatching batching = new ReadIndexBatching(
        command -> {
          throw new RejectedExecutionException("closed");
        }, 64, request -> CompletableFuture.completedFuture(ReadIndexReplyProto.getDefaultInstance()));

    final CompletableFuture<ReadIndexReplyProto> rejected = batching.submit(null);
    assertReadIndexException(rejected);

    final CompletableFuture<ReadIndexReplyProto> afterClose = batching.submit(null);
    assertReadIndexException(afterClose);
  }

  @Test
  void testSubmitAfterCloseCompletesExceptionally() {
    final AtomicInteger readIndexCount = new AtomicInteger();
    final ReadIndexBatching batching = new ReadIndexBatching(
        Runnable::run, 64, request -> {
          readIndexCount.incrementAndGet();
          return new CompletableFuture<ReadIndexReplyProto>();
        });

    batching.close();

    final CompletableFuture<ReadIndexReplyProto> reply = batching.submit(null);
    final ExecutionException e = Assertions.assertThrows(ExecutionException.class, reply::get);
    Assertions.assertTrue(e.getCause() instanceof ReadIndexException);
    Assertions.assertEquals(0, readIndexCount.get());
  }

  private static void assertReadIndexException(CompletableFuture<ReadIndexReplyProto> future) throws Exception {
    final ExecutionException e = Assertions.assertThrows(ExecutionException.class, future::get);
    Assertions.assertTrue(e.getCause() instanceof ReadIndexException);
  }

  private static class CapturingExecutor implements Executor {
    private final List<Runnable> tasks = new ArrayList<>();

    public int getTaskCount() {
      return tasks.size();
    }

    @Override
    public void execute(Runnable command) {
      tasks.add(command);
    }

    void runNext() {
      tasks.remove(0).run();
    }
  }
}
