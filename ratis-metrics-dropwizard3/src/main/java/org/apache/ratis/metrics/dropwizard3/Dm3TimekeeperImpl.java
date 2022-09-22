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
package org.apache.ratis.metrics.dropwizard3;

import org.apache.ratis.metrics.Timekeeper;
import com.codahale.metrics.Timer;

/**
 * The default implementation of {@link Timekeeper} by the shaded {@link Timer}.
 */
public class Dm3TimekeeperImpl implements Timekeeper {
  private final Timer timer;

  Dm3TimekeeperImpl(Timer timer) {
    this.timer = timer;
  }

  public Timer getTimer() {
    return timer;
  }

  @Override
  public Context time() {
    final Timer.Context context = timer.time();
    return context::stop;
  }
}
