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
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/** For batching excessive log messages. */
public final class BatchLogger {
  private static final Logger LOG = LoggerFactory.getLogger(BatchLogger.class);

  private BatchLogger() {
  }

  public interface Key {}

  private static final class UniqueId {
    private final Key key;
    private final String name;

    private UniqueId(Key key, String name) {
      this.key = Objects.requireNonNull(key, "key == null");
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
      return Objects.equals(this.key, that.key)
          && Objects.equals(this.name, that.name);
    }

    @Override
    public int hashCode() {
      return key.hashCode() ^ name.hashCode();
    }
  }

  private static final class BatchedLogEntry {
    private Consumer<String> logOp;
    private Timestamp startTime = Timestamp.currentTime();
    private int count = 0;

    private synchronized void execute() {
      if (count <= 1) {
        return;
      }
      logOp.accept(String.format(" (Repeated %d times in the last %s)",
          count, startTime.elapsedTime().toString(TimeUnit.SECONDS, 3)));
      startTime = null;
    }

    private synchronized boolean tryStartBatch(Consumer<String> op) {
      if (startTime == null) { // already executed
        op.accept("");
        return false;
      }
      logOp = op;
      count++;
      return count == 1;
    }
  }

  private static final TimeoutExecutor SCHEDULER = TimeoutExecutor.getInstance();
  private static final ConcurrentMap<UniqueId, BatchedLogEntry> LOG_CACHE = new ConcurrentHashMap<>();

  public static void warn(Key key, String name, Consumer<String> op, TimeDuration batchDuration) {
    warn(key, name, op, batchDuration, true);
  }

  public static void warn(Key key, String name, Consumer<String> op, TimeDuration batchDuration, boolean shouldBatch) {
    if (!shouldBatch || batchDuration.isNonPositive()) {
      op.accept("");
      return;
    }

    final UniqueId id = new UniqueId(key, name);
    final BatchedLogEntry entry = LOG_CACHE.computeIfAbsent(id, k -> new BatchedLogEntry());

    if (entry.tryStartBatch(op)) {
      // print the first warn log on batch start
      op.accept("");
      SCHEDULER.onTimeout(batchDuration,
          () -> Optional.ofNullable(LOG_CACHE.remove(id)).ifPresent(BatchedLogEntry::execute),
          LOG, () -> "print batched exception failed on " + op);
    }
  }
}
