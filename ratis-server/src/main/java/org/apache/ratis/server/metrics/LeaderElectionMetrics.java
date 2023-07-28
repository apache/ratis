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

package org.apache.ratis.server.metrics;

import org.apache.ratis.metrics.LongCounter;
import org.apache.ratis.metrics.MetricRegistryInfo;
import org.apache.ratis.metrics.RatisMetricRegistry;
import org.apache.ratis.metrics.RatisMetrics;
import org.apache.ratis.metrics.Timekeeper;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.util.Timestamp;

import java.util.Optional;
import java.util.function.LongSupplier;

/**
 * Class to update the metrics related to Leader Election.
 */
public final class LeaderElectionMetrics extends RatisMetrics {

  public static final String RATIS_LEADER_ELECTION_METRICS = "leader_election";
  public static final String RATIS_LEADER_ELECTION_METRICS_DESC = "Metrics for Ratis Leader Election.";

  public static final String LEADER_ELECTION_COUNT_METRIC = "electionCount";
  public static final String LEADER_ELECTION_TIMEOUT_COUNT_METRIC = "timeoutCount";
  public static final String LEADER_ELECTION_TIME_TAKEN = "electionTime";
  public static final String LAST_LEADER_ELAPSED_TIME = "lastLeaderElapsedTime";
  public static final String TRANSFER_LEADERSHIP_COUNT_METRIC = "transferLeadershipCount";

  public static final String LAST_LEADER_ELECTION_ELAPSED_TIME = "lastLeaderElectionElapsedTime";

  private final LongCounter electionCount = getRegistry().counter(LEADER_ELECTION_COUNT_METRIC);
  private final LongCounter timeoutCount = getRegistry().counter(LEADER_ELECTION_TIMEOUT_COUNT_METRIC);
  private final LongCounter transferLeadershipCount = getRegistry().counter(TRANSFER_LEADERSHIP_COUNT_METRIC);

  private final Timekeeper electionTime = getRegistry().timer(LEADER_ELECTION_TIME_TAKEN);

  private volatile Timestamp lastElectionTime;

  private LeaderElectionMetrics(RaftGroupMemberId serverId, LongSupplier getLastLeaderElapsedTimeMs) {
    super(createRegistry(serverId));

    getRegistry().gauge(LAST_LEADER_ELAPSED_TIME, () -> getLastLeaderElapsedTimeMs::getAsLong);
    getRegistry().gauge(LAST_LEADER_ELECTION_ELAPSED_TIME,
        () -> () -> Optional.ofNullable(lastElectionTime).map(Timestamp::elapsedTimeMs).orElse(-1L));
  }

  public static RatisMetricRegistry createRegistry(RaftGroupMemberId serverId) {
    return create(new MetricRegistryInfo(serverId.toString(),
        RATIS_APPLICATION_NAME_METRICS, RATIS_LEADER_ELECTION_METRICS,
        RATIS_LEADER_ELECTION_METRICS_DESC));
  }

  public static LeaderElectionMetrics getLeaderElectionMetrics(
      RaftGroupMemberId serverId, LongSupplier getLastLeaderElapsedTimeMs) {
    return new LeaderElectionMetrics(serverId, getLastLeaderElapsedTimeMs);
  }

  public void onNewLeaderElectionCompletion() {
    electionCount.inc();
    lastElectionTime = Timestamp.currentTime();
  }

  public Timekeeper getLeaderElectionTimer() {
    return electionTime;
  }

  public void onLeaderElectionTimeout() {
    timeoutCount.inc();
  }

  public void onTransferLeadership() {
    transferLeadershipCount.inc();
  }
}
