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
import org.apache.ratis.protocol.exceptions.ReadException;
import org.apache.ratis.protocol.exceptions.TimeoutIOException;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.MemoizedSupplier;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.TimeoutExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Consumer;

/** For supporting linearizable read. */
class ReadRequests {
  private static final Logger LOG = LoggerFactory.getLogger(ReadRequests.class);

  static class ReadIndexQueue {
    private final TimeoutExecutor scheduler = TimeoutExecutor.getInstance();
    private final NavigableMap<Long, CompletableFuture<Long>> sorted = new ConcurrentSkipListMap<>();
    private final TimeDuration readTimeout;

    ReadIndexQueue(TimeDuration readTimeout) {
      this.readTimeout = readTimeout;
    }

    CompletableFuture<Long> add(long readIndex) {
      final MemoizedSupplier<CompletableFuture<Long>> supplier = MemoizedSupplier.valueOf(CompletableFuture::new);
      final CompletableFuture<Long> f = sorted.computeIfAbsent(readIndex, i -> supplier.get());

      if (supplier.isInitialized()) {
        scheduler.onTimeout(readTimeout, () -> handleTimeout(readIndex),
            LOG, () -> "Failed to handle read timeout for index " + readIndex);
      }
      return f;
    }

    private void handleTimeout(long readIndex) {
      Optional.ofNullable(sorted.remove(readIndex)).ifPresent(consumer -> {
        consumer.completeExceptionally(
          new ReadException(new TimeoutIOException("Read timeout for index " + readIndex)));
      });
    }

    void complete(Long appliedIndex) {
      for(;;) {
        if (sorted.isEmpty()) {
          return;
        }
        final Long first = sorted.firstKey();
        if (first == null || first > appliedIndex) {
          return;
        }
        Optional.ofNullable(sorted.remove(first)).ifPresent(f -> f.complete(appliedIndex));
      }
    }
  }

  private final ReadIndexQueue readIndexQueue;
  private final StateMachine stateMachine;

  ReadRequests(RaftProperties properties, StateMachine stateMachine) {
    this.readIndexQueue = new ReadIndexQueue(RaftServerConfigKeys.Read.timeout(properties));
    this.stateMachine = stateMachine;
  }

  Consumer<Long> getAppliedIndexConsumer() {
    return readIndexQueue::complete;
  }

  CompletableFuture<Long> waitToAdvance(long readIndex) {
    if (stateMachine.getLastAppliedTermIndex().getIndex() >= readIndex) {
      return CompletableFuture.completedFuture(readIndex);
    }
    return readIndexQueue.add(readIndex);
  }
}
