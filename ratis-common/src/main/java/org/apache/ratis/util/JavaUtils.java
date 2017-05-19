/*
 * *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.ratis.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * General Java utility methods.
 */
public interface JavaUtils {
  Logger LOG = LoggerFactory.getLogger(JavaUtils.class);

  /**
   * Invoke {@link Callable#call()} and, if there any,
   * wrap the checked exception by {@link RuntimeException}.
   */
  static <T> T callAsUnchecked(Callable<T> callable) {
    try {
      return callable.call();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Get the value from the future and then consume it.
   */
  static <T> void getAndConsume(CompletableFuture<T> future, Consumer<T> consumer) {
    final T t;
    try {
      t = future.get();
    } catch (Exception ignored) {
      LOG.warn("Failed to get()", ignored);
      return;
    }
    consumer.accept(t);
  }
}
