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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/*
 * Wait strategy for incremental wait based on error count.
 *
 */
public class ErrorWaitStrategy {
  private final AtomicLong errCount = new AtomicLong(0);
  
  private List<ErrTimeWait> waitStrategyList = new ArrayList<>();
  
  /*
  * create a strategy of wait with list of pair of error count and wait
  * in milli sec, in order of ascending error count,
  * eg: 5,1000,10,5000 where 5 is error count and 1000 is wait time, and so on
  * 
  * @param conf wait strategy configuration
   */
  public ErrorWaitStrategy(String conf) {
    String[] split = conf.split(",");
    if (split.length % 2 != 0) {
      return;
    }
    
    for (int i = 0; i < split.length; i += 2) {
      try {
        ErrTimeWait errTimeWait = new ErrTimeWait();
        errTimeWait.errCount = Long.parseLong(split[i]);
        errTimeWait.waitTime = Long.parseLong(split[i + 1]);
        waitStrategyList.add(errTimeWait);
      } catch (NumberFormatException ex) {
        // ignore
      }
    }
    // need check in reverse order if err count crosses limit
    Collections.reverse(waitStrategyList);
  }
  
  public void incrErrCount() {
    errCount.getAndIncrement();
  }
  
  public void resetErrCount() {
    errCount.set(0);
  }
  
  public long waitTimeMs() {
    if (errCount.get() == 0) {
      return 0;
    }
    
    for (ErrTimeWait errTimeWait : waitStrategyList) {
      if (errCount.get() > errTimeWait.errCount) {
        return errTimeWait.waitTime;
      }
    }
    return 0;
  }
  
  private class ErrTimeWait {
    Long errCount;
    Long waitTime;
  }
}
