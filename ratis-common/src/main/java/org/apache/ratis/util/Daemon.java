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

import java.util.Objects;

public class Daemon extends Thread {
  {
    setDaemon(true);
  }

  /** Construct a daemon thread with flexible arguments. */
  protected Daemon(Builder builder) {
    super(builder.threadGroup, builder.runnable);
    setName(builder.name);
  }

  /** @return a {@link Builder}. */
  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private String name;
    private Runnable runnable;
    private ThreadGroup threadGroup;

    public Builder setName(String name) {
      this.name = name;
      return this;
    }

    public Builder setRunnable(Runnable runnable) {
      this.runnable = runnable;
      return this;
    }

    public Builder setThreadGroup(ThreadGroup threadGroup) {
      this.threadGroup = threadGroup;
      return this;
    }

    public Daemon build() {
      Objects.requireNonNull(name, "name == null");
      return new Daemon(this);
    }
  }
}
