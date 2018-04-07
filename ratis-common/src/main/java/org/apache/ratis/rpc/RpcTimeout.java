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
package org.apache.ratis.rpc;

import org.apache.ratis.shaded.com.google.common.base.Supplier;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RpcTimeout {
  private static final Logger LOG = LoggerFactory.getLogger(RpcTimeout.class);
  private ScheduledExecutorService timeoutScheduler = null;
  private final TimeDuration callTimeout;
  private int numUsers = 0;

  public RpcTimeout(TimeDuration callTimeout) {
    this.callTimeout = callTimeout;
  }

  public synchronized void addUser() {
    if (timeoutScheduler == null) {
      timeoutScheduler = Executors.newScheduledThreadPool(1);
    }
    numUsers++;
  }

  public synchronized void removeUser() {
    numUsers--;
    if (timeoutScheduler != null && numUsers == 0) {
      timeoutScheduler.shutdown();
      timeoutScheduler = null;
    }
  }

  public synchronized void onTimeout(Runnable task, Supplier<String> errorMsg) {
    Preconditions.assertTrue(timeoutScheduler != null);
    TimeUnit unit = callTimeout.getUnit();
    timeoutScheduler.schedule(() -> {
      try {
        task.run();
      } catch (Throwable t) {
        LOG.error(errorMsg.get(), t);
      }
    }, callTimeout.toInt(unit), unit);
  }

  public TimeDuration getCallTimeout() {
    return callTimeout;
  }
}
