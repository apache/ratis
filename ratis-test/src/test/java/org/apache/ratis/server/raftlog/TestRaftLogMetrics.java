/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ratis.server.raftlog;

import com.codahale.metrics.Timer;
import org.apache.ratis.BaseTest;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.metrics.JVMMetrics;
import org.apache.ratis.metrics.RatisMetricRegistry;
import org.apache.ratis.metrics.RatisMetrics;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.server.metrics.RaftLogMetricsBase;
import org.apache.ratis.server.simulation.MiniRaftClusterWithSimulatedRpc;
import org.apache.ratis.server.storage.RaftStorageTestUtils;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.TimeDuration;
import org.junit.Assert;
import org.junit.Test;

import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.ratis.metrics.RatisMetrics.RATIS_APPLICATION_NAME_METRICS;
import static org.apache.ratis.server.metrics.RaftLogMetricsBase.*;
import static org.apache.ratis.server.metrics.SegmentedRaftLogMetrics.*;

public class TestRaftLogMetrics extends BaseTest
    implements MiniRaftClusterWithSimulatedRpc.FactoryGet {
  static {
    JVMMetrics.initJvmMetrics(TimeDuration.valueOf(10, TimeUnit.SECONDS));
  }

  public static final int NUM_SERVERS = 3;

  {
    getProperties().setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        MetricsStateMachine.class, StateMachine.class);
  }

  static class MetricsStateMachine extends BaseStateMachine {
    static MetricsStateMachine get(RaftServer.Division s) {
      return (MetricsStateMachine)s.getStateMachine();
    }

    private final AtomicInteger flushCount = new AtomicInteger();

    int getFlushCount() {
      return flushCount.get();
    }

    @Override
    public CompletableFuture<Void> flush(long index) {
      flushCount.incrementAndGet();
      return CompletableFuture.completedFuture(null);
    }
  }

  @Test
  public void testRaftLogMetrics() throws Exception {
    try(final MiniRaftCluster cluster = newCluster(NUM_SERVERS)) {
      cluster.start();
      runTestRaftLogMetrics(cluster);
    }
  }

  static void runTestRaftLogMetrics(MiniRaftCluster cluster) throws Exception {
    int numMsg = 2;
    final RaftTestUtil.SimpleMessage[] messages = RaftTestUtil.SimpleMessage.create(numMsg);

    try (final RaftClient client = cluster.createClient()) {
      for (RaftTestUtil.SimpleMessage message : messages) {
        client.io().send(message);
      }
    }

    // For leader, flush must happen before client can get replies.
    assertFlushCount(cluster.getLeader());
    assertRaftLogWritePathMetrics(cluster.getLeader());

    // For followers, flush can be lagged behind.  Attempt multiple times.
    for(RaftServer.Division f : cluster.getFollowers()) {
      JavaUtils.attempt(() -> assertFlushCount(f), 10, HUNDRED_MILLIS, f.getId() + "-assertFlushCount", null);
      // We have already waited enough for follower metrics to populate.
      assertRaftLogWritePathMetrics(f);
    }

    // Wait for commits to happen on leader
    JavaUtils.attempt(() -> assertCommitCount(cluster.getLeader(), numMsg), 10, HUNDRED_MILLIS, cluster.getLeader().getId() + "-assertCommitCount", null);
  }

  static void assertCommitCount(RaftServer.Division server, int expectedMsgs) {
    final RatisMetricRegistry rlm = ((RatisMetrics)server.getRaftLog().getRaftLogMetrics()).getRegistry();
    long stmCount = rlm.counter(STATE_MACHINE_LOG_ENTRY_COUNT).getCount();
    Assert.assertEquals(expectedMsgs, stmCount);
  }

  static void assertFlushCount(RaftServer.Division server) throws Exception {
    final String flushTimeMetric = RaftStorageTestUtils.getLogFlushTimeMetric(server.getMemberId().toString());
    final RatisMetricRegistry ratisMetricRegistry = RaftLogMetricsBase.getLogWorkerMetricRegistry(server.getMemberId());
    Timer tm = (Timer) ratisMetricRegistry.get(RAFT_LOG_FLUSH_TIME);
    Assert.assertNotNull(tm);

    final MetricsStateMachine stateMachine = MetricsStateMachine.get(server);
    final int expectedFlush = stateMachine.getFlushCount();

    JavaUtils.attemptRepeatedly(() -> {
      Assert.assertEquals(expectedFlush, tm.getCount());
      return null;
    }, 50, HUNDRED_MILLIS, "expectedFlush == tm.getCount()", null);

    Assert.assertTrue(tm.getMeanRate() > 0);

    // Test jmx
    ObjectName oname = new ObjectName(RATIS_APPLICATION_NAME_METRICS, "name", flushTimeMetric);
    Assert.assertEquals(expectedFlush,
        ((Long) ManagementFactory.getPlatformMBeanServer().getAttribute(oname, "Count"))
            .intValue());
  }

  static void assertRaftLogWritePathMetrics(RaftServer.Division server) throws Exception {
    final String syncTimeMetric = RaftStorageTestUtils.getRaftLogFullMetric(server.getMemberId().toString(), RAFT_LOG_SYNC_TIME);
    final RatisMetricRegistry ratisMetricRegistry = RaftLogMetricsBase.getLogWorkerMetricRegistry(server.getMemberId());

    //Test sync count
    Timer tm = (Timer) ratisMetricRegistry.get(RAFT_LOG_SYNC_TIME);
    Assert.assertNotNull(tm);
    final MetricsStateMachine stateMachine = MetricsStateMachine.get(server);
    final int expectedFlush = stateMachine.getFlushCount();
    Assert.assertEquals(expectedFlush, tm.getCount()); // Ideally, flushCount should be same as syncCount.
    Assert.assertTrue(tm.getMeanRate() > 0);

    // Test jmx. Just testing one metric's JMX is good enough.
    ObjectName oname = new ObjectName(RATIS_APPLICATION_NAME_METRICS, "name", syncTimeMetric);
    Assert.assertEquals(expectedFlush,
        ((Long) ManagementFactory.getPlatformMBeanServer().getAttribute(oname, "Count"))
            .intValue());

    long cacheMissCount = ratisMetricRegistry.counter(RAFT_LOG_CACHE_MISS_COUNT).getCount();
    Assert.assertEquals(0, cacheMissCount);

    long cacheHitsCount = ratisMetricRegistry.counter(RAFT_LOG_CACHE_HIT_COUNT).getCount();
    Assert.assertTrue(cacheHitsCount > 0);

    Assert.assertTrue(ratisMetricRegistry.counter(RAFT_LOG_FLUSH_COUNT).getCount() > 0);
    Assert.assertTrue(ratisMetricRegistry.counter(RAFT_LOG_APPEND_ENTRY_COUNT).getCount() > 0);

    Timer appendLatencyTimer = ratisMetricRegistry.timer(RAFT_LOG_APPEND_ENTRY_LATENCY);
    Assert.assertTrue(appendLatencyTimer.getMeanRate() > 0);

    Timer enqueuedTimer = ratisMetricRegistry.timer(RAFT_LOG_TASK_QUEUE_TIME);
    Assert.assertTrue(enqueuedTimer.getMeanRate() > 0);

    Timer queueingDelayTimer = ratisMetricRegistry.timer(RAFT_LOG_TASK_ENQUEUE_DELAY);
    Assert.assertTrue(queueingDelayTimer.getMeanRate() > 0);

    Timer executionTimer = ratisMetricRegistry.timer(String.format(RAFT_LOG_TASK_EXECUTION_TIME, "writelog"));
    Assert.assertTrue(executionTimer.getMeanRate() > 0);

    Assert.assertNotNull(ratisMetricRegistry.get(RAFT_LOG_DATA_QUEUE_SIZE));
    Assert.assertNotNull(ratisMetricRegistry.get(RAFT_LOG_WORKER_QUEUE_SIZE));
    Assert.assertNotNull(ratisMetricRegistry.get(RAFT_LOG_SYNC_BATCH_SIZE));
  }
}