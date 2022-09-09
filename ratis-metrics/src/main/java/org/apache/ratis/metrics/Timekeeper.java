/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ratis.metrics;

import org.apache.ratis.util.UncheckedAutoCloseable;

import java.util.Optional;

@FunctionalInterface
public interface Timekeeper {
  UncheckedAutoCloseable NOOP = () -> {};

  static UncheckedAutoCloseable start(Timekeeper timekeeper) {
    return Optional.ofNullable(timekeeper)
        .map(Timekeeper::time)
        .map(Context::toAutoCloseable)
        .orElse(NOOP);
  }

  @FunctionalInterface
  interface Context {
    long stop();

    default UncheckedAutoCloseable toAutoCloseable() {
      return this::stop;
    }
  }

  Context time();
}
