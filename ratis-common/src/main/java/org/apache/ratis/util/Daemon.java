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

import javax.annotation.Nullable;
import java.util.concurrent.atomic.AtomicReference;

public class Daemon extends Thread {
  {
    setDaemon(true);
  }

  /** If the thread meets an uncaught exception, this field will be set. */
  private final AtomicReference<Throwable> throwable = new AtomicReference<>(null);
  protected ErrorRecorded statedServer;

  /** Construct a daemon thread. */
  // TODO(jiacheng): Consolidate all constructors
  public Daemon() {
    super();
    setUncaughtExceptionHandler((thread, t) -> {
      onError(t);
    });
  }

  public Daemon(String name, ErrorRecorded server) {
    this();
    this.setName(name);
    this.statedServer = server;
  }

  /** Construct a daemon thread with the given runnable. */
  public Daemon(Runnable runnable) {
    this(runnable, runnable.toString());
  }

  /** Construct a daemon thread with the given runnable. */
  public Daemon(Runnable runnable, String name) {
    super(runnable);
    this.setName(name);
  }

  public Daemon(Runnable runnable, String name, ErrorRecorded server) {
    this(runnable, name);
    this.statedServer = server;
  }

  /**
   * Handles the uncaught error on thread crashing.
   *
   * @param t the crashing error
   */
  public void onError(Throwable t) {
    throwable.set(t);
    if (statedServer != null) {
      // Rely on the server to log
      statedServer.setError(t);
      // TODO(jiacheng): Transition the server state to ERROR
    }

    // TODO(jiacheng): should i set the lifecycle to close and exit? or it is possible to recover?
    //  Do a RaftServer state transition in a heartbeat thread
    // TODO(jiacheng): what if this thread is created in a threadpool?
  }

  @Nullable
  public Throwable getError() {
    return throwable.get();
  }
}
