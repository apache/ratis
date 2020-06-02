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

import org.apache.ratis.metrics.MetricRegistryInfo;
import org.apache.ratis.metrics.RatisMetricRegistry;
import org.apache.ratis.metrics.RatisMetrics;
import org.apache.ratis.server.raftlog.RaftLogIndex;
import org.apache.ratis.statemachine.StateMachine;

import java.util.function.LongSupplier;

import com.codahale.metrics.Timer;

/**
 * Metrics Registry for the State Machine Updater. One instance per group.
 */
public final class StateMachineMetrics extends RatisMetrics {

  public static final String RATIS_STATEMACHINE_METRICS = "state_machine";
  public static final String RATIS_STATEMACHINE_METRICS_DESC = "Metrics for State Machine Updater";

  public static final String STATEMACHINE_APPLIED_INDEX_GAUGE = "appliedIndex";
  public static final String STATEMACHINE_APPLY_COMPLETED_GAUGE = "applyCompletedIndex";
  public static final String STATEMACHINE_TAKE_SNAPSHOT_TIMER = "takeSnapshot";

  public static StateMachineMetrics getStateMachineMetrics(
      RaftServerImpl server, RaftLogIndex appliedIndex,
      StateMachine stateMachine) {

    String serverId = server.getMemberId().toString();
    LongSupplier getApplied = appliedIndex::get;
    LongSupplier getApplyCompleted =
        () -> (stateMachine.getLastAppliedTermIndex() == null) ? -1
            : stateMachine.getLastAppliedTermIndex().getIndex();

    return new StateMachineMetrics(serverId, getApplied, getApplyCompleted);
  }

  private StateMachineMetrics(String serverId, LongSupplier getApplied,
      LongSupplier getApplyCompleted) {
    registry = getMetricRegistryForStateMachine(serverId);
    registry.gauge(STATEMACHINE_APPLIED_INDEX_GAUGE,
        () -> () -> getApplied.getAsLong());
    registry.gauge(STATEMACHINE_APPLY_COMPLETED_GAUGE,
        () -> () -> getApplyCompleted.getAsLong());
  }

  private RatisMetricRegistry getMetricRegistryForStateMachine(String serverId) {
    return create(new MetricRegistryInfo(serverId,
        RATIS_APPLICATION_NAME_METRICS,
        RATIS_STATEMACHINE_METRICS, RATIS_STATEMACHINE_METRICS_DESC));
  }

  public Timer getTakeSnapshotTimer() {
    return registry.timer(STATEMACHINE_TAKE_SNAPSHOT_TIMER);
  }

}