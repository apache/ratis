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
package org.apache.hadoop.raft;

import java.util.Random;

public abstract class RaftConstants {
  static final int ELECTION_TIMEOUT_MIN_MS = 150;
  static final int ELECTION_TIMEOUT_MAX_MS = 300;
  static final int ELECTION_SLEEP_TIME_MS = 50;

  static final int RPC_TIMEOUT_MIN_MS = 150;
  static final int RPC_TIMEOUT_MAX_MS = 300;
  static final int RPC_SLEEP_TIME_MS = 50;

  static final int ELECTION_TIMEOUT_MS_WIDTH
      = ELECTION_TIMEOUT_MAX_MS - ELECTION_TIMEOUT_MIN_MS;

  static final Random RANDOM = new Random();

  static int getRandomElectionWaitTime() {
    return RANDOM.nextInt(ELECTION_TIMEOUT_MS_WIDTH) + ELECTION_TIMEOUT_MIN_MS;
  }
}
