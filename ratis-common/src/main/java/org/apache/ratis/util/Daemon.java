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

public class Daemon extends Thread {
  static final Logger LOG = LoggerFactory.getLogger(Daemon.class);
  public static final Thread.UncaughtExceptionHandler LOG_EXCEPTION =
      (t, e) -> LOG.error(t.getName() + " threw an uncaught exception", e);

  {
    setDaemon(true);
  }

  /** Construct a daemon thread with flexible arguments. */
  protected Daemon(Builder builder) {
    super(builder.runnable);
    setName(builder.name);
    Optional.ofNullable(builder.uncaughtExceptionHandler).ifPresent(this::setUncaughtExceptionHandler);
  }

  /** @return a {@link Builder}. */
  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private String name;
    private Runnable runnable;
    // By default, uncaught exceptions are just logged without further actions
    private UncaughtExceptionHandler uncaughtExceptionHandler = LOG_EXCEPTION;

    public Builder setName(String name) {
      this.name = name;
      return this;
    }

    public Builder setRunnable(Runnable runnable) {
      this.runnable = runnable;
      return this;
    }

    public Builder setUncaughtExceptionHandler(UncaughtExceptionHandler exceptionHandler) {
      this.uncaughtExceptionHandler = exceptionHandler;
      return this;
    }

    public Daemon build() {
      Objects.requireNonNull(name, "name == null");
      return new Daemon(this);
    }
  }
}
