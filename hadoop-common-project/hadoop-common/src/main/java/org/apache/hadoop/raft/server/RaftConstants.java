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
package org.apache.hadoop.raft.server;

import com.google.common.annotations.VisibleForTesting;

import java.util.Random;

public interface RaftConstants {
  String RAFT_SERVER_KERBEROS_PRINCIPAL_KEY
      = "raft.server.kerberos.principal";
  String RAFT_CLIENT_KERBEROS_PRINCIPAL_KEY
      = "raft.client.kerberos.principal";
  String RAFT_SERVER_PROTOCOL_NAME
      = "org.apache.hadoop.raft.server.protocol.RaftServerProtocol";
  String RAFT_CLIENT_PROTOCOL_NAME
      = "org.apache.hadoop.raft.protocol.RaftClientProtocol";

  long INVALID_LOG_INDEX = -1;
  byte LOG_TERMINATE_BYTE = 0;

  int LOG_SEGMENT_MAX_SIZE = 1024 * 1024 * 8;
  int SNAPSHOT_CHUNK_MAX_SIZE = 1024 * 1024 * 16;

  @VisibleForTesting
  int ELECTION_TIMEOUT_MIN_MS = 150;
  int ELECTION_TIMEOUT_MAX_MS = 300;

  int RPC_TIMEOUT_MIN_MS = 150;
  int RPC_TIMEOUT_MAX_MS = 300;
  int RPC_SLEEP_TIME_MS = 25;

  int ELECTION_TIMEOUT_MS_WIDTH = ELECTION_TIMEOUT_MAX_MS - ELECTION_TIMEOUT_MIN_MS;

  int LOG_FORCE_SYNC_NUM = 128;

  /**
   * When bootstrapping a new peer, If the gap between the match index of the
   * peer and the leader's latest committed index is less than this gap, we
   * treat the peer as caught-up.
   */
  int STAGING_CATCHUP_GAP = 10; // TODO: a small number for test
  long STAGING_NOPROGRESS_TIMEOUT = 2 * RPC_TIMEOUT_MAX_MS;

  Random RANDOM = new Random();

  static int getRandomElectionWaitTime() {
    return RANDOM.nextInt(ELECTION_TIMEOUT_MS_WIDTH) + ELECTION_TIMEOUT_MIN_MS;
  }

  enum StartupOption {
    FORMAT("format"),
    REGULAR("regular");

    private final String option;

    StartupOption(String arg) {
      this.option = arg;
    }

    public static StartupOption getOption(String arg) {
      for (StartupOption s : StartupOption.values()) {
        if (s.option.equals(arg)) {
          return s;
        }
      }
      return REGULAR;
    }
  }
}
