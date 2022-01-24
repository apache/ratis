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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class is a partial implementation of {@link java.util.concurrent.locks.Condition}.
 * Only some of the await and signal methods are implemented.
 *
 * This class is threadsafe.
 */
public class AwaitForSignal {
  private final String name;

  private final AtomicReference<CompletableFuture<Void>> future = new AtomicReference<>(new CompletableFuture<>());

  public AwaitForSignal(Object name) {
    this.name = name + "-" + JavaUtils.getClassSimpleName(getClass());
  }

  /** The same as {@link java.util.concurrent.locks.Condition#await()} */
  public void await() throws InterruptedException {
    try {
      future.get().get();
    } catch (ExecutionException e) {
      throw new IllegalStateException(e);
    }
  }

  /** The same as {@link java.util.concurrent.locks.Condition#await(long, TimeUnit)} */
  public boolean await(long time, TimeUnit unit) throws InterruptedException {
    if (time <= 0) {
      return false;
    }
    try {
      future.get().get(time, unit);
    } catch (ExecutionException e) {
      throw new IllegalStateException(e);
    } catch (TimeoutException ignored) {
      return false;
    }
    return true;
  }

  /** The same as {@link java.util.concurrent.locks.Condition#signal()} */
  public void signal() {
    future.getAndSet(new CompletableFuture<>()).complete(null);
  }

  @Override
  public String toString() {
    return name;
  }
}