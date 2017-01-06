/**
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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;

/**
 * Wrap a lock with the {@link AutoCloseable} interface
 * so that the {@link #close()} method will unlock the lock.
 */
public class AutoCloseableLock implements AutoCloseable {
  /**
   * Acquire the given lock and then wrap it with {@link AutoCloseableLock}
   * so that the given lock can be released by calling {@link #close()},
   * or by using a {@code try}-with-resources statement as shown below.
   *
   * <pre> {@code
   * try(AutoCloseableLock acl = AutoCloseableLock.acquire(lock)) {
   *   ...
   * }}</pre>
   */
  public static AutoCloseableLock acquire(final Lock lock) {
    lock.lock();
    return new AutoCloseableLock(lock);
  }

  private final Lock underlying;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  private AutoCloseableLock(Lock underlying) {
    this.underlying = underlying;
  }

  /** Unlock the underlying lock.  This method is idempotent. */
  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      underlying.unlock();
    }
  }
}
