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

package org.apache.ratis.util;

import org.slf4j.Logger;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

/** For batching excessive log messages. */
public final class BatchLogger {

  private BatchLogger() {
  }

  public static final class UniqueId {
    private final StackTraceElement traceElement;
    private final String name;

    private UniqueId(StackTraceElement traceElement, String name) {
      this.traceElement = traceElement;
      this.name = name;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      } else if (!(obj instanceof UniqueId)) {
        return false;
      }

      final UniqueId that = (UniqueId) obj;
      return Objects.equals(this.traceElement, that.traceElement)
          && Objects.equals(this.name, that.name);
    }

    @Override
    public int hashCode() {
      return traceElement.hashCode() ^ name.hashCode();
    }
  }

  public static UniqueId makeUniqueId(String name) {
    return new UniqueId(JavaUtils.getCallerStackTraceElement(), name);
  }

  private static final class BatchedLogEntry {
    private final Consumer<String> log;
    private final Runnable logOp;
    private Timestamp startTime = null;
    private int count = 0;

    private BatchedLogEntry(Consumer<String> log, Runnable logOp) {
      this.log = log;
      this.logOp = logOp;
    }

    private synchronized void execute() {
      if (count <= 1) {
        return;
      }
      log.accept(String.format("Received %s logs of following in the last %s seconds:",
          count, startTime.elapsedTime()));
      logOp.run();
      startTime = null;
    }

    private synchronized boolean tryStartBatch() {
      if (startTime == null) {
        startTime = Timestamp.currentTime();
        count = 1;
        return true;
      }
      count++;
      return false;
    }
  }

  private static final TimeoutExecutor SCHEDULER = TimeoutExecutor.getInstance();
  private static final ConcurrentMap<UniqueId, BatchedLogEntry> LOG_CACHE = new ConcurrentHashMap<>();

  public static void warn(Logger log, UniqueId id, Runnable op, TimeDuration batchDuration) {
    warn(log, id, op, batchDuration, true);
  }

  public static void warn(Logger log, UniqueId id, Runnable op, TimeDuration batchDuration, boolean shouldBatch) {
    if (!shouldBatch || batchDuration.isNonPositive()) {
      op.run();
      return;
    }

    final BatchedLogEntry entry = LOG_CACHE.computeIfAbsent(id, key -> new BatchedLogEntry(log::warn, op));

    if (entry.tryStartBatch()) {
      // print the first warn log on batch start
      op.run();
      SCHEDULER.onTimeout(batchDuration,
          () -> Optional.ofNullable(LOG_CACHE.remove(id)).ifPresent(BatchedLogEntry::execute),
          log, () -> "print batched exception failed on " + op);
    }
  }
}
