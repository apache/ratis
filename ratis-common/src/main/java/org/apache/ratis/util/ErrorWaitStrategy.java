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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ratis.retry.MultipleLinearRandomRetry;
import org.apache.ratis.retry.RetryPolicy;

/*
 * Wait strategy for incremental wait based on error count.
 */
public class ErrorWaitStrategy implements RetryPolicy.Event {
  private final MultipleLinearRandomRetry randomRetry;
  private final AtomicLong errCount = new AtomicLong(0);
  
  /*
  * create a strategy of wait with MultipleLinearRandomRetry.
  * The format of the string is "t_1, n_1, t_2, n_2, ..." as input
  * for MultipleLinearRandomRetry.
  * 
  * @param retryVal wait strategy value
   */
  public ErrorWaitStrategy(String retryVal) {
    randomRetry = MultipleLinearRandomRetry.parseCommaSeparated(retryVal);
  }
  
  public void incrErrCount() {
    errCount.getAndIncrement();
  }
  
  public void resetErrCount() {
    errCount.set(0);
  }
  
  public long waitTimeMs() {
    RetryPolicy.Action action = randomRetry.handleAttemptFailure(this);
    if (!action.shouldRetry()) {
      return 0;
    }
    
    return action.getSleepTime().toLong(TimeUnit.MILLISECONDS);
  }

  @Override
  public int getAttemptCount() {
    return errCount.intValue();
  }
}
