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

import org.apache.ratis.util.function.CheckedRunnable;
import org.slf4j.Logger;

import java.util.function.Consumer;
import java.util.function.Supplier;

/** Execute timeout tasks. */
public interface TimeoutExecutor {
  int MAXIMUM_POOL_SIZE =  8;
  static TimeoutExecutor getInstance() {
    return TimeoutTimer.getInstance();
  }

  /** @return the number of scheduled but not completed timeout tasks. */
  int getTaskCount();

  /**
   * Schedule a timeout task.
   *
   * @param timeout the timeout value.
   * @param task the task to run when timeout.
   * @param errorHandler to handle the error, if there is any.
   */
  <THROWABLE extends Throwable> void onTimeout(
      TimeDuration timeout, CheckedRunnable<THROWABLE> task, Consumer<THROWABLE> errorHandler);

  /** When timeout, run the task.  Log the error, if there is any. */
  default void onTimeout(TimeDuration timeout, CheckedRunnable<?> task, Logger log, Supplier<String> errorMessage) {
    onTimeout(timeout, task, t -> log.error(errorMessage.get(), t));
  }
}
