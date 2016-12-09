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
package org.apache.raft.grpc;

import org.apache.raft.client.RaftClientConfigKeys;

public interface RaftGrpcConfigKeys {
  String PREFIX = "raft.grpc";

  String RAFT_GRPC_SERVER_PORT_KEY = PREFIX + ".server.port";
  int RAFT_GRPC_SERVER_PORT_DEFAULT = 0;

  String RAFT_GRPC_MESSAGE_MAXSIZE_KEY = PREFIX + ".message.maxsize";
  int RAFT_GRPC_MESSAGE_MAXSIZE_DEFAULT = 64 * 1024 * 1024; // 64 MB

  String RAFT_GRPC_LEADER_MAX_OUTSTANDING_APPENDS_KEY =
      PREFIX + "leader.max.outstanding.appends";
  int RAFT_GRPC_LEADER_MAX_OUTSTANDING_APPENDS_DEFAULT = 128;

  String RAFT_GRPC_CLIENT_MAX_OUTSTANDING_APPENDS_KEY =
      PREFIX + "client.max.outstanding.appends";
  int RAFT_GRPC_CLIENT_MAX_OUTSTANDING_APPENDS_DEFAULT = 128;

  String RAFT_OUTPUTSTREAM_BUFFER_SIZE_KEY = "raft.outputstream.buffer.size";
  int RAFT_OUTPUTSTREAM_BUFFER_SIZE_DEFAULT = 64 * 1024;

  String RAFT_OUTPUTSTREAM_MAX_RETRY_TIMES_KEY = "raft.outputstream.max.retry.times";
  int RAFT_OUTPUTSTREAM_MAX_RETRY_TIMES_DEFAULT = 5;

  String RAFT_OUTPUTSTREAM_RETRY_INTERVAL_KEY = "raft.outputstream.retry.interval";
  long RAFT_OUTPUTSTREAM_RETRY_INTERVAL_DEFAULT = RaftClientConfigKeys.RAFT_RPC_TIMEOUT_MS_DEFAULT;
}
