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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

public class AutoCloseableReadWriteLock {
  private final Object name;
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
  private final AtomicInteger depth = new AtomicInteger();

  public AutoCloseableReadWriteLock(Object name) {
    this.name = name;
  }

  public AutoCloseableLock readLock(StackTraceElement caller, Consumer<String> log) {
    final AutoCloseableLock readLock = AutoCloseableLock.acquire(lock.readLock(),
        () -> logLocking(name, caller, true, false, log));

    logLocking(name, caller, true, true, log);
    return readLock;
  }

  public AutoCloseableLock writeLock(StackTraceElement caller, Consumer<String> log) {
    final AutoCloseableLock writeLock = AutoCloseableLock.acquire(lock.writeLock(),
        () -> logLocking(name, caller, false, false, log));

    logLocking(name, caller, false, true, log);
    return writeLock;
  }

  private void logLocking(Object name, StackTraceElement caller, boolean read, boolean acquire, Consumer<String> log) {
    if (caller != null && log != null) {
      final int d = acquire? depth.getAndIncrement(): depth.decrementAndGet();
      final StringBuilder b = new StringBuilder();
      for(int i = 0; i < d; i++) {
        b.append("  ");
      }
      if (name != null) {
        b.append(name).append(": ");
      }
      b.append(read? "readLock ": "writeLock ")
          .append(acquire ? "ACQUIRED ": "RELEASED ")
          .append(depth).append(" by ");
      final String className = caller.getClassName();
      final int i = className.lastIndexOf('.');
      b.append(i >= 0? className.substring(i + 1): className).append(".").append(caller.getMethodName());
      log.accept(b.toString());
    }
  }
}
