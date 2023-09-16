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

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.leader.FollowerInfo;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.Timestamp;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class LeaderLease {

  private final long leaseTimeoutMs;
  // TODO invalidate leader lease when stepDown / transferLeader
  private final AtomicReference<Timestamp> lease = new AtomicReference<>(Timestamp.currentTime());

  LeaderLease(RaftProperties properties) {
    final double leaseRatio = RaftServerConfigKeys.Read.leaderLeaseTimeoutRatio(properties);
    Preconditions.assertTrue(leaseRatio > 0.0 && leaseRatio <= 1.0,
        "leader ratio should sit in (0,1], now is " + leaseRatio);
    this.leaseTimeoutMs = RaftServerConfigKeys.Rpc.timeoutMin(properties)
        .multiply(leaseRatio)
        .toIntExact(TimeUnit.MILLISECONDS);
  }

  boolean isValid() {
    return lease.get().elapsedTimeMs() < leaseTimeoutMs;
  }

  /**
   * try extending the lease based on group heartbeats
   * @param old nullable
   */
  void extend(List<FollowerInfo> current, List<FollowerInfo> old, Predicate<List<RaftPeerId>> hasMajority) {
    final List<RaftPeerId> activePeers =
        // check the latest heartbeats of all peers (including those in transitional)
        Stream.concat(current.stream(), Optional.ofNullable(old).map(List::stream).orElse(Stream.empty()))
            .filter(f -> f.getLastRespondedAppendEntriesSendTime().elapsedTimeMs() < leaseTimeoutMs)
            .map(FollowerInfo::getId)
            .collect(Collectors.toList());

    if (!hasMajority.test(activePeers)) {
      return;
    }

    lease.set(Timestamp.earliest(getMaxOfMajorityAck(current), getMaxOfMajorityAck(old)));
  }

  /**
   * return maximum timestamp at when the majority of followers are known to be active
   * return {@link Timestamp#currentTime()} if peers are empty
   */
  private Timestamp getMaxOfMajorityAck(List<FollowerInfo> peers) {
    if (peers == null || peers.isEmpty()) {
      return Timestamp.currentTime();
    }

    final List<Timestamp> lastRespondedAppendEntriesSendTimes = peers.stream()
        .map(FollowerInfo::getLastRespondedAppendEntriesSendTime)
        .sorted()
        .collect(Collectors.toList());

    return lastRespondedAppendEntriesSendTimes.get(lastRespondedAppendEntriesSendTimes.size() / 2);
  }
}
