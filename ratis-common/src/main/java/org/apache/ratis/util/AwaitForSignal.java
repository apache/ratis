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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class is a partial implementation of {@link java.util.concurrent.locks.Condition}.
 * Only some of the await and signal methods are implemented.
 * <p>
 * This class is threadsafe.
 */
public class AwaitForSignal {
  private final String name;
  private final Lock lock = new ReentrantLock();
  private final Condition condition = lock.newCondition();
  private final AtomicReference<AtomicBoolean> signaled = new AtomicReference<>(new AtomicBoolean());

  public AwaitForSignal(Object name) {
    this.name = name + "-" + JavaUtils.getClassSimpleName(getClass());
  }

  /** The same as {@link java.util.concurrent.locks.Condition#await()} */
  public void await() throws InterruptedException {
    lock.lock();
    try {
      for (final AtomicBoolean s = signaled.get(); !s.get(); ) {
        condition.await();
      }
    } finally {
      lock.unlock();
    }
  }

  /** The same as {@link java.util.concurrent.locks.Condition#await(long, TimeUnit)} */
  public boolean await(long time, TimeUnit unit) throws InterruptedException {
    if (time <= 0) {
      return false;
    }
    lock.lock();
    try {
      return condition.await(time, unit);
    } finally {
      lock.unlock();
    }
  }

  /** The same as {@link java.util.concurrent.locks.Condition#signal()} */
  public void signal() {
    lock.lock();
    try {
      signaled.getAndSet(new AtomicBoolean()).set(true);
      condition.signalAll();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public String toString() {
    return name;
  }
}