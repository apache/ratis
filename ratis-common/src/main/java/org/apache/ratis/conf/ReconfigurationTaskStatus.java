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

package org.apache.ratis.conf;

import java.util.Map;
import java.util.Optional;
import org.apache.ratis.conf.ReconfigurationUtil.PropertyChange;

public class ReconfigurationTaskStatus {
  private long startTime;
  private long endTime;
  private final Map<ReconfigurationUtil.PropertyChange, Optional<String>> status;

  public ReconfigurationTaskStatus(long startTime, long endTime,
      Map<ReconfigurationUtil.PropertyChange, Optional<String>> status) {
    this.startTime = startTime;
    this.endTime = endTime;
    this.status = status;
  }

  /**
   * Return true if
   *   - A reconfiguration task has finished or
   *   - an active reconfiguration task is running.
   * @return true if startTime &gt; 0; false if not.
   */
  public boolean hasTask() {
    return startTime > 0;
  }

  /**
   * Return true if the latest reconfiguration task has finished and there is
   * no another active task running.
   * @return true if endTime &gt; 0; false if not.
   */
  public boolean stopped() {
    return endTime > 0;
  }

  public long getStartTime() {
    return startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public final Map<PropertyChange, Optional<String>> getStatus() {
    return status;
  }
}
