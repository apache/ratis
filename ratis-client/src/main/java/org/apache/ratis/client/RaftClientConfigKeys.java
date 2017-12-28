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
package org.apache.ratis.client;

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.util.TimeDuration;

import java.util.concurrent.TimeUnit;

import static org.apache.ratis.conf.ConfUtils.*;

public interface RaftClientConfigKeys {
  String PREFIX = "raft.client";

  interface Rpc {
    String PREFIX = RaftClientConfigKeys.PREFIX + ".rpc";

    String TIMEOUT_KEY = PREFIX + ".timeout";
    TimeDuration TIMEOUT_DEFAULT = TimeDuration.valueOf(300, TimeUnit.MILLISECONDS);

    static TimeDuration timeout(RaftProperties properties) {
      return getTimeDuration(properties.getTimeDuration(TIMEOUT_DEFAULT.getUnit()),
          TIMEOUT_KEY, TIMEOUT_DEFAULT);
    }
  }

  interface Async {
    String PREFIX = RaftClientConfigKeys.PREFIX + ".async";

    String MAX_OUTSTANDING_REQUESTS_KEY = PREFIX + ".outstanding-requests.max";
    int MAX_OUTSTANDING_REQUESTS_DEFAULT = 100;

    static int maxOutstandingRequests(RaftProperties properties) {
      return getInt(properties::getInt, MAX_OUTSTANDING_REQUESTS_KEY,
          MAX_OUTSTANDING_REQUESTS_DEFAULT, requireMin(2));
    }

    static void setMaxOutstandingRequests(RaftProperties properties, int outstandingRequests) {
      setInt(properties::setInt, MAX_OUTSTANDING_REQUESTS_KEY, outstandingRequests);
    }

    String SCHEDULER_THREADS_KEY = PREFIX + ".scheduler-threads";
    int SCHEDULER_THREADS_DEFAULT = 3;

    static int schedulerThreads(RaftProperties properties) {
      return getInt(properties::getInt, SCHEDULER_THREADS_KEY,
          SCHEDULER_THREADS_DEFAULT, requireMin(1));
    }

    static void setSchedulerThreads(RaftProperties properties, int schedulerThreads) {
      setInt(properties::setInt, SCHEDULER_THREADS_KEY, schedulerThreads);
    }
  }

  static void main(String[] args) {
    printAll(RaftClientConfigKeys.class);
  }
}
