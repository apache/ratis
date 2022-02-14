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
package org.apache.ratis.client;

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.apache.ratis.conf.ConfUtils.*;

public interface RaftClientConfigKeys {
  Logger LOG = LoggerFactory.getLogger(RaftClientConfigKeys.class);

  static Consumer<String> getDefaultLog() {
    return LOG::debug;
  }

  String PREFIX = "raft.client";

  interface Rpc {
    String PREFIX = RaftClientConfigKeys.PREFIX + ".rpc";

    String REQUEST_TIMEOUT_KEY = PREFIX + ".request.timeout";
    TimeDuration REQUEST_TIMEOUT_DEFAULT = TimeDuration.valueOf(3000, TimeUnit.MILLISECONDS);
    static TimeDuration requestTimeout(RaftProperties properties) {
      return getTimeDuration(properties.getTimeDuration(REQUEST_TIMEOUT_DEFAULT.getUnit()),
          REQUEST_TIMEOUT_KEY, REQUEST_TIMEOUT_DEFAULT, getDefaultLog());
    }
    static void setRequestTimeout(RaftProperties properties, TimeDuration timeoutDuration) {
      setTimeDuration(properties::setTimeDuration, REQUEST_TIMEOUT_KEY, timeoutDuration);
    }

    String WATCH_REQUEST_TIMEOUT_KEY = PREFIX + ".watch.request.timeout";
    TimeDuration WATCH_REQUEST_TIMEOUT_DEFAULT =
        TimeDuration.valueOf(10000, TimeUnit.MILLISECONDS);
    static TimeDuration watchRequestTimeout(RaftProperties properties) {
      return getTimeDuration(properties.getTimeDuration(WATCH_REQUEST_TIMEOUT_DEFAULT.getUnit()),
          WATCH_REQUEST_TIMEOUT_KEY, WATCH_REQUEST_TIMEOUT_DEFAULT, getDefaultLog());
    }
    static void setWatchRequestTimeout(RaftProperties properties,
        TimeDuration timeoutDuration) {
      setTimeDuration(properties::setTimeDuration, WATCH_REQUEST_TIMEOUT_KEY, timeoutDuration);
    }
  }

  interface Async {
    String PREFIX = RaftClientConfigKeys.PREFIX + ".async";

    String OUTSTANDING_REQUESTS_MAX_KEY = PREFIX + ".outstanding-requests.max";
    int OUTSTANDING_REQUESTS_MAX_DEFAULT = 100;
    static int outstandingRequestsMax(RaftProperties properties) {
      return getInt(properties::getInt, OUTSTANDING_REQUESTS_MAX_KEY,
          OUTSTANDING_REQUESTS_MAX_DEFAULT, getDefaultLog(), requireMin(2));
    }
    static void setOutstandingRequestsMax(RaftProperties properties, int outstandingRequests) {
      setInt(properties::setInt, OUTSTANDING_REQUESTS_MAX_KEY, outstandingRequests);
    }

    interface Experimental {
      String PREFIX = Async.PREFIX + "." + JavaUtils.getClassSimpleName(Experimental.class).toLowerCase();

      String SEND_DUMMY_REQUEST_KEY = PREFIX + ".send-dummy-request";
      boolean SEND_DUMMY_REQUEST_DEFAULT = true;
      static boolean sendDummyRequest(RaftProperties properties) {
        return getBoolean(properties::getBoolean, SEND_DUMMY_REQUEST_KEY, SEND_DUMMY_REQUEST_DEFAULT, getDefaultLog());
      }
      static void setSendDummyRequest(RaftProperties properties, boolean sendDummyRequest) {
        setBoolean(properties::setBoolean, SEND_DUMMY_REQUEST_KEY, sendDummyRequest);
      }
    }
  }

  interface DataStream {
    String PREFIX = RaftClientConfigKeys.PREFIX + ".data-stream";

    String OUTSTANDING_REQUESTS_MAX_KEY = PREFIX + ".outstanding-requests.max";
    int OUTSTANDING_REQUESTS_MAX_DEFAULT = 10;
    static int outstandingRequestsMax(RaftProperties properties) {
      return getInt(properties::getInt, OUTSTANDING_REQUESTS_MAX_KEY,
          OUTSTANDING_REQUESTS_MAX_DEFAULT, getDefaultLog(), requireMin(2));
    }
    static void setOutstandingRequestsMax(RaftProperties properties, int outstandingRequests) {
      setInt(properties::setInt, OUTSTANDING_REQUESTS_MAX_KEY, outstandingRequests);
    }

    String REQUEST_TIMEOUT_KEY = PREFIX + ".request.timeout";
    TimeDuration REQUEST_TIMEOUT_DEFAULT = TimeDuration.valueOf(3000, TimeUnit.MILLISECONDS);
    static TimeDuration requestTimeout(RaftProperties properties) {
      return getTimeDuration(properties.getTimeDuration(REQUEST_TIMEOUT_DEFAULT.getUnit()),
          REQUEST_TIMEOUT_KEY, REQUEST_TIMEOUT_DEFAULT, getDefaultLog());
    }
    static void setRequestTimeout(RaftProperties properties, TimeDuration timeoutDuration) {
      setTimeDuration(properties::setTimeDuration, REQUEST_TIMEOUT_KEY, timeoutDuration);
    }

    String SEND_REQUEST_WORKERS_KEY = PREFIX + ".send.request.workers";
    int SEND_REQUEST_WORKERS_DEFAULT = 10;
    static int sendRequestWorker(RaftProperties properties) {
      return getInt(properties::getInt, SEND_REQUEST_WORKERS_KEY,
          SEND_REQUEST_WORKERS_DEFAULT, getDefaultLog(), requireMin(1));
    }
    static void setSendRequestWorker(RaftProperties properties, int sendRequestWorkers) {
      setInt(properties::setInt, SEND_REQUEST_WORKERS_KEY, sendRequestWorkers);
    }
  }

  interface MessageStream {
    String PREFIX = RaftClientConfigKeys.PREFIX + ".message-stream";

    String SUBMESSAGE_SIZE_KEY = PREFIX + ".submessage-size";
    SizeInBytes SUBMESSAGE_SIZE_DEFAULT = SizeInBytes.valueOf("1MB");
    static SizeInBytes submessageSize(RaftProperties properties) {
      return getSizeInBytes(properties::getSizeInBytes,
          SUBMESSAGE_SIZE_KEY, SUBMESSAGE_SIZE_DEFAULT, getDefaultLog());
    }
    static void setSubmessageSize(RaftProperties properties, SizeInBytes submessageSize) {
      setSizeInBytes(properties::set, SUBMESSAGE_SIZE_KEY, submessageSize, requireMin(SizeInBytes.ONE_KB));
    }
    static void setSubmessageSize(RaftProperties properties) {
      setSubmessageSize(properties, SUBMESSAGE_SIZE_DEFAULT);
    }
  }

  static void main(String[] args) {
    printAll(RaftClientConfigKeys.class);
  }
}
