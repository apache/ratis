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

import org.apache.ratis.metrics.RatisMetricRegistry;
import org.apache.ratis.server.metrics.RatisMetricNames;
import org.apache.ratis.server.metrics.RatisMetrics;
import org.apache.ratis.server.raftlog.RaftLogIndex;
import org.apache.ratis.statemachine.StateMachine;

import java.util.function.LongSupplier;

/**
 * Metrics Registry for the State Machine Updater. One instance per group.
 */
public final class StateMachineMetrics {
  private RatisMetricRegistry registry = null;

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

    registry = RatisMetrics.getMetricRegistryForStateMachine(serverId);
    registry.gauge(RatisMetricNames.STATEMACHINE_APPLIED_INDEX_GAUGE,
        () -> () -> getApplied.getAsLong());
    registry.gauge(RatisMetricNames.STATEMACHINE_APPLY_COMPLETED_GAUGE,
        () -> () -> getApplyCompleted.getAsLong());
  }
}