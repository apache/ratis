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

import javax.annotation.Nullable;
import java.util.concurrent.atomic.AtomicReference;

public class Daemon extends Thread {
  {
    setDaemon(true);
  }

  /** If the thread meets an uncaught exception, this field will be set. */
  private final AtomicReference<Throwable> throwable = new AtomicReference<>(null);
  private ErrorRecorded statedServer;

  /** Construct a daemon thread with no arguments, left only for extension. */
  public Daemon() {
    super();
    setUncaughtExceptionHandler((thread, t) -> {
      onError(t);
    });
  }

  /** Construct a daemon thread with flexible arguments. */
  public Daemon(Builder builder) {
    super(builder.runnable);
    setName(builder.name);
    this.statedServer = builder.statedServer;
    setUncaughtExceptionHandler((thread, t) -> {
      onError(t);
    });
  }

  /**
   * This will be invoked on uncaught exceptions.
   * Necessary bookkeeping or graceful exit logics should be put here.
   *
   * @param t the crashing error
   */
  public void onError(Throwable t) {
    throwable.set(t);
    if (statedServer != null) {
      // Rely on the server to log
      statedServer.setError(t);
    }
  }

  @Nullable
  public Throwable getError() {
    return throwable.get();
  }

  public static class Builder {
    private final String name;
    private Runnable runnable;
    private ErrorRecorded statedServer;

    public Builder(String name) {
      this.name = name;
    }

    public Builder setRunnable(Runnable runnable) {
      this.runnable = runnable;
      return this;
    }

    public Builder setStatedServer(ErrorRecorded er) {
      this.statedServer = er;
      return this;
    }

    public Daemon build() {
      return new Daemon(this);
    }
  }
}
