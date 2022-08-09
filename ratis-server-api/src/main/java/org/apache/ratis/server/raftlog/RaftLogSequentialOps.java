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
package org.apache.ratis.server.raftlog;

import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.server.RaftConfiguration;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.StringUtils;
import org.apache.ratis.util.function.CheckedSupplier;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Sequential operations in {@link RaftLog}.
 *
 * All methods in this class MUST be invoked by a single thread at any time.
 * The threads can be different in different time.
 * The same thread may invoke any of the methods again and again.
 * In other words, two or more threads invoking these methods (the same method or different methods)
 * at the same time is not allowed since the sequence of invocations cannot be guaranteed.
 *
 * All methods in this class are asynchronous in the sense that the underlying I/O operations are asynchronous.
 */
interface RaftLogSequentialOps {
  class Runner {
    private final Object name;
    private final AtomicReference<Thread> runner = new AtomicReference<>();

    Runner(Supplier<String> name) {
      this.name = StringUtils.stringSupplierAsObject(name);
    }

    /**
     * Run the given operation sequentially.
     * This method can be invoked by different threads but only one thread at any given time is allowed.
     * The same thread can call this method multiple times.
     *
     * @throws IllegalStateException if this runner is already running another operation.
     */
    <OUTPUT, THROWABLE extends Throwable> OUTPUT runSequentially(
        CheckedSupplier<OUTPUT, THROWABLE> operation) throws THROWABLE {
      final Thread current = Thread.currentThread();
      // update only if the runner is null
      final Thread previous = runner.getAndUpdate(prev -> prev != null? prev: current);
      if (previous == null) {
        // The current thread becomes the runner.
        try {
          return operation.get();
        } finally {
          // prev is expected to be current
          final Thread got = runner.getAndUpdate(prev -> prev != current? prev: null);
          Preconditions.assertTrue(got == current,
              () -> name + ": Unexpected runner " + got + " != " + current);
        }
      } else if (previous == current) {
        // The current thread is already the runner.
        return operation.get();
      } else {
        throw new IllegalStateException(
            name + ": Already running a method by " + previous + ", current=" + current);
      }
    }
  }

  /**
   * Append asynchronously a log entry for the given term and transaction.
   * Used by the leader.
   *
   * Note that the underlying I/O operation is submitted but may not be completed when this method returns.
   *
   * @return the index of the new log entry.
   */
  long append(long term, TransactionContext transaction) throws StateMachineException;

  /**
   * Append asynchronously a log entry for the given term and configuration
   * Used by the leader.
   *
   * Note that the underlying I/O operation is submitted but may not be completed when this method returns.
   *
   * @return the index of the new log entry.
   */
  long append(long term, RaftConfiguration configuration);

  /**
   * Append asynchronously a log entry for the given term and commit index
   * unless the given commit index is an index of a metadata entry
   * Used by the leader.
   *
   * Note that the underlying I/O operation is submitted but may not be completed when this method returns.
   *
   * @return the index of the new log entry if it is appended;
   *         otherwise, return {@link org.apache.ratis.server.raftlog.RaftLog#INVALID_LOG_INDEX}.
   */
  long appendMetadata(long term, long commitIndex);

  /**
   * Append asynchronously an entry.
   * Used by the leader and the followers.
   */
  CompletableFuture<Long> appendEntry(LogEntryProto entry);

  /**
   * The same as append(Arrays.asList(entries)).
   *
   * @deprecated use {@link #append(List)}
   */
  @Deprecated
  default List<CompletableFuture<Long>> append(LogEntryProto... entries) {
    return append(Arrays.asList(entries));
  }

  /**
   * Append asynchronously all the given log entries.
   * Used by the followers.
   *
   * If an existing entry conflicts with a new one (same index but different terms),
   * delete the existing entry and all entries that follow it (§5.3).
   */
  List<CompletableFuture<Long>> append(List<LogEntryProto> entries);

  /**
   * Truncate asynchronously the log entries till the given index (inclusively).
   * Used by the leader and the followers.
   */
  CompletableFuture<Long> truncate(long index);
}
