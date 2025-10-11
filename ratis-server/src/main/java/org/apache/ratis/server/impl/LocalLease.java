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
package org.apache.ratis.server.impl;

import org.apache.ratis.protocol.RaftPeerId;

import java.util.concurrent.atomic.AtomicLong;

/** LocalLease can be used for followers to check if it's updated to Leader.
 */
public class LocalLease {
  private final AtomicLong leaderCommit = new AtomicLong(-1);
  private volatile long lastHbNanos = 0L;
  private volatile long leaseTerm   = -1L;
  private volatile RaftPeerId leaseLeader = null;

  public LocalLease() {

  }

  void onAppend(long term, RaftPeerId leader, long commitIdx) {
    if (leaseTerm != term || leaseLeader == null || !leaseLeader.equals(leader)) {
      leaseTerm = term;
      leaseLeader = leader;
      leaderCommit.set(commitIdx);
    } else {
      long prev;
      do {
        prev = leaderCommit.get();
      } while (commitIdx > prev && !leaderCommit.compareAndSet(prev, commitIdx));
    }
    lastHbNanos = System.nanoTime();
  }

  public long getLastHbNanos() {
    return lastHbNanos;
  }

  public AtomicLong getLeaderCommit() {
    return leaderCommit;
  }
}
