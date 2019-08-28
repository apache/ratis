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

import static org.apache.ratis.server.metrics.RatisMetricNames.LAST_LEADER_ELAPSED_TIME;
import static org.apache.ratis.server.metrics.RatisMetricNames.LEADER_ELECTION_COUNT_METRIC;
import static org.apache.ratis.server.metrics.RatisMetricNames.LEADER_ELECTION_LATENCY;
import static org.apache.ratis.server.metrics.RatisMetricNames.LEADER_ELECTION_TIMEOUT_COUNT_METRIC;

import org.apache.ratis.metrics.RatisMetricRegistry;
import org.apache.ratis.server.impl.RaftServerImpl;

/**
 * Class to update the metrics related to Leader Election.
 */
public final class LeaderElectionMetrics {

  private long leaderElectionCompletionLatency = 0L;
  private RatisMetricRegistry ratisMetricRegistry;

  private LeaderElectionMetrics(RaftServerImpl raftServer) {
    this.ratisMetricRegistry = RatisMetrics.getMetricRegistryForLeaderElection(raftServer.getMemberId().toString());
    ratisMetricRegistry.gauge(LEADER_ELECTION_LATENCY, () -> () -> leaderElectionCompletionLatency);
    ratisMetricRegistry.gauge(LAST_LEADER_ELAPSED_TIME, () -> () -> raftServer.getState().getLastLeaderElapsedTimeMs());
  }

  public static LeaderElectionMetrics getLeaderElectionMetrics(RaftServerImpl raftServer) {
    return new LeaderElectionMetrics(raftServer);
  }

  public void onNewLeaderElection() {
    ratisMetricRegistry.counter(LEADER_ELECTION_COUNT_METRIC).inc();
  }

  public void onLeaderElectionCompletion(long elapsedTime) {
    this.leaderElectionCompletionLatency = elapsedTime;
  }

  public void onLeaderElectionTimeout() {
    ratisMetricRegistry.counter(LEADER_ELECTION_TIMEOUT_COUNT_METRIC).inc();
  }
}
