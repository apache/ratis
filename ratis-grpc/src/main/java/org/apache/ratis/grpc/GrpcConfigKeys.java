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
package org.apache.ratis.grpc;

import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;

import static org.apache.ratis.conf.ConfUtils.*;

public interface GrpcConfigKeys {
  String PREFIX = "raft.grpc";

  interface Server {
    String PREFIX = GrpcConfigKeys.PREFIX + ".server";

    String PORT_KEY = PREFIX + ".port";
    int PORT_DEFAULT = 0;
    static int port(RaftProperties properties) {
      return getInt(properties::getInt,
          PORT_KEY, PORT_DEFAULT, requireMin(0), requireMax(65536));
    }
    static void setPort(RaftProperties properties, int port) {
      setInt(properties::setInt, PORT_KEY, port);
    }

    String MESSAGE_SIZE_MAX_KEY = PREFIX + ".message.size.max";
    SizeInBytes MESSAGE_SIZE_MAX_DEFAULT = SizeInBytes.valueOf("64MB");
    static SizeInBytes messageSizeMax(RaftProperties properties) {
      return getSizeInBytes(properties::getSizeInBytes,
          MESSAGE_SIZE_MAX_KEY, MESSAGE_SIZE_MAX_DEFAULT);
    }

    String LEADER_OUTSTANDING_APPENDS_MAX_KEY = PREFIX + ".leader.outstanding.appends.max";
    int LEADER_OUTSTANDING_APPENDS_MAX_DEFAULT = 128;
    static int leaderOutstandingAppendsMax(RaftProperties properties) {
      return getInt(properties::getInt,
          LEADER_OUTSTANDING_APPENDS_MAX_KEY, LEADER_OUTSTANDING_APPENDS_MAX_DEFAULT, requireMin(0));
    }
  }

  interface OutputStream {
    String PREFIX = GrpcConfigKeys.PREFIX + ".outputstream";

    String BUFFER_SIZE_KEY = PREFIX + ".buffer.size";
    SizeInBytes BUFFER_SIZE_DEFAULT = SizeInBytes.valueOf("64KB");
    static SizeInBytes bufferSize(RaftProperties properties) {
      return getSizeInBytes(properties::getSizeInBytes,
          BUFFER_SIZE_KEY, BUFFER_SIZE_DEFAULT);
    }
    static void setBufferSize(RaftProperties properties, SizeInBytes bufferSize) {
      setSizeInBytes(properties::set, BUFFER_SIZE_KEY, bufferSize);
    }

    String RETRY_TIMES_KEY = PREFIX + ".retry.times";
    int RETRY_TIMES_DEFAULT = 5;
    static int retryTimes(RaftProperties properties) {
      return getInt(properties::getInt,
          RETRY_TIMES_KEY, RETRY_TIMES_DEFAULT, requireMin(1));
    }

    String RETRY_INTERVAL_KEY = PREFIX + ".retry.interval";
    TimeDuration RETRY_INTERVAL_DEFAULT = RaftClientConfigKeys.Rpc.TIMEOUT_DEFAULT;
    static TimeDuration retryInterval(RaftProperties properties) {
      return getTimeDuration(properties.getTimeDuration(RETRY_INTERVAL_DEFAULT.getUnit()),
          RETRY_INTERVAL_KEY, RETRY_INTERVAL_DEFAULT);
    }

    String OUTSTANDING_APPENDS_MAX_KEY = PREFIX + ".outstanding.appends.max";
    int OUTSTANDING_APPENDS_MAX_DEFAULT = 128;
    static int outstandingAppendsMax(RaftProperties properties) {
      return getInt(properties::getInt,
          OUTSTANDING_APPENDS_MAX_KEY, OUTSTANDING_APPENDS_MAX_DEFAULT, requireMin(0));
    }
  }

  static void main(String[] args) {
    printAll(GrpcConfigKeys.class);
  }
}
