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

import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.StringUtils;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.LongUnaryOperator;

/**
 * Indices of a raft log such as commit index, match index, etc.
 *
 * This class is thread safe.
 */
public class RaftLogIndex {
  private final Object name;
  private final AtomicLong index;

  public RaftLogIndex(Object name, long initialValue) {
    this.name = name;
    this.index = new AtomicLong(initialValue);
  }

  public long get() {
    return index.get();
  }

  public boolean setUnconditionally(long newIndex, Consumer<Object> log) {
    final long old = index.getAndSet(newIndex);
    log.accept(StringUtils.stringSupplierAsObject(() -> name + ": setUnconditionally " + old + " -> " + newIndex));
    return old != newIndex;
  }

  public boolean updateUnconditionally(LongUnaryOperator update, Consumer<Object> log) {
    final long old = index.getAndUpdate(update);
    final long newIndex = update.applyAsLong(old);
    log.accept(StringUtils.stringSupplierAsObject(() -> name + ": updateUnconditionally " + old + " -> " + newIndex));
    return old != newIndex;
  }

  public boolean updateIncreasingly(long newIndex, Consumer<Object> log) {
    final long old = index.getAndSet(newIndex);
    Preconditions.assertTrue(old <= newIndex,
        () -> "Failed to updateIncreasingly for " + name + ": " + old + " -> " + newIndex);
    log.accept(StringUtils.stringSupplierAsObject(() -> name + ": updateIncreasingly " + old + " -> " + newIndex));
    return old != newIndex;
  }

  public boolean updateToMax(long newIndex, Consumer<Object> log) {
    final long old = index.getAndUpdate(oldIndex -> Math.max(oldIndex, newIndex));
    final boolean updated = old < newIndex;
    log.accept(StringUtils.stringSupplierAsObject(
        () -> name + ": updateToMax old=" + old + ", new=" + newIndex + ", updated? " + updated));
    return updated;
  }

  public long incrementAndGet(Consumer<Object> log) {
    final long newIndex = index.incrementAndGet();
    log.accept(StringUtils.stringSupplierAsObject(
        () -> name + ": incrementAndGet " + (newIndex-1) + " -> " + newIndex));
    return newIndex;
  }

  @Override
  public String toString() {
    return name + ":" + index;
  }
}
