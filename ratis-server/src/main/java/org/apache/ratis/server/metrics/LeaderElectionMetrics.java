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

import org.apache.ratis.metrics.MetricRegistryInfo;
import org.apache.ratis.metrics.RatisMetricRegistry;
import org.apache.ratis.metrics.RatisMetrics;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.util.Timestamp;

import com.codahale.metrics.Timer;

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

  public static final String LAST_LEADER_ELECTION_ELAPSED_TIME =
      "lastLeaderElectionElapsedTime";
  private Timestamp lastElectionTime;

  private LeaderElectionMetrics(RaftServerImpl raftServer) {
    this.registry = getMetricRegistryForLeaderElection(raftServer.getMemberId().toString());
    registry.gauge(LAST_LEADER_ELAPSED_TIME, () -> () -> raftServer.getState().getLastLeaderElapsedTimeMs());
    registry.gauge(LAST_LEADER_ELECTION_ELAPSED_TIME, () -> () -> {
      if (lastElectionTime == null) {
        return -1L;
      } else {
        return lastElectionTime.elapsedTimeMs();
      }
    });
  }

  private RatisMetricRegistry getMetricRegistryForLeaderElection(String serverId) {
    return create(new MetricRegistryInfo(serverId,
        RATIS_APPLICATION_NAME_METRICS, RATIS_LEADER_ELECTION_METRICS,
        RATIS_LEADER_ELECTION_METRICS_DESC));
  }

  public static LeaderElectionMetrics getLeaderElectionMetrics(RaftServerImpl raftServer) {
    return new LeaderElectionMetrics(raftServer);
  }

  public void onNewLeaderElectionCompletion() {
    registry.counter(LEADER_ELECTION_COUNT_METRIC).inc();
    lastElectionTime = Timestamp.currentTime();
  }

  public Timer getLeaderElectionTimer() {
    return registry.timer(LEADER_ELECTION_TIME_TAKEN);
  }

  public void onLeaderElectionTimeout() {
    registry.counter(LEADER_ELECTION_TIMEOUT_COUNT_METRIC).inc();
  }
}
