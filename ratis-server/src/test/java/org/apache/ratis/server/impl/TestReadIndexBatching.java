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
import org.apache.ratis.protocol.exceptions.ReadIndexException;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.TimeoutExecutor;
import org.apache.ratis.util.function.CheckedRunnable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

class TestReadIndexBatching {
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

  private static class NoOpTimeoutExecutor implements TimeoutExecutor {
    @Override
    public int getTaskCount() {
      return 0;
    }

    @Override
    public <THROWABLE extends Throwable> void onTimeout(
        TimeDuration timeout, CheckedRunnable<THROWABLE> task, Consumer<THROWABLE> errorHandler) {
    }

    @Override
    public void onTimeout(TimeDuration timeout, CheckedRunnable<?> task, Logger log, Supplier<String> errorMessage) {
    }
  }
}
