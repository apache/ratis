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

public abstract class RaftConstants {
  public static final long INVALID_LOG_INDEX = -1;
  public static final byte LOG_TERMINATE_BYTE = 0;

  public static final int LOG_SEGMENT_SIZE = 1024 * 1024 * 8;

  @VisibleForTesting
  public static final int ELECTION_TIMEOUT_MIN_MS = 150;
  public static final int ELECTION_TIMEOUT_MAX_MS = 300;

  static final int RPC_TIMEOUT_MIN_MS = 150;
  static final int RPC_TIMEOUT_MAX_MS = 300;
  static final int RPC_SLEEP_TIME_MS = 25;

  static final int ELECTION_TIMEOUT_MS_WIDTH
      = ELECTION_TIMEOUT_MAX_MS - ELECTION_TIMEOUT_MIN_MS;

  /**
   * When bootstrapping a new peer, If the gap between the match index of the
   * peer and the leader's latest committed index is less than this gap, we
   * treat the peer as caught-up.
   */
  static final int STAGING_CATCHUP_GAP = 10; // TODO: a small number for test
  static final long STAGING_NOPROGRESS_TIMEOUT = 2 * RPC_TIMEOUT_MAX_MS;

  static final Random RANDOM = new Random();

  static int getRandomElectionWaitTime() {
    return RANDOM.nextInt(ELECTION_TIMEOUT_MS_WIDTH) + ELECTION_TIMEOUT_MIN_MS;
  }

  public enum StartupOption {
    FORMAT("format"),
    REGULAR("regular"),
    BOOTSTRAPSTANDBY("bootstrap");

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
