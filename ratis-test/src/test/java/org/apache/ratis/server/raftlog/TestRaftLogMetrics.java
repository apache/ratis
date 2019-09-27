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

import static org.apache.ratis.server.metrics.RatisMetricNames.RAFT_LOG_FLUSH_TIME;
import static org.apache.ratis.server.metrics.RatisMetricNames.RAFT_LOG_SYNC_TIME;

import com.codahale.metrics.Timer;
import org.apache.log4j.Level;
import org.apache.ratis.BaseTest;
import org.apache.ratis.MiniRaftCluster;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.metrics.RatisMetricRegistry;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.metrics.RatisMetrics;
import org.apache.ratis.server.simulation.MiniRaftClusterWithSimulatedRpc;
import org.apache.ratis.server.storage.RaftStorageTestUtils;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.LogUtils;
import org.junit.Assert;
import org.junit.Test;

import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

public class TestRaftLogMetrics extends BaseTest
    implements MiniRaftClusterWithSimulatedRpc.FactoryGet {

  {
    LogUtils.setLogLevel(RaftServerImpl.LOG, Level.DEBUG);
  }

  public static final int NUM_SERVERS = 3;

  {
    getProperties().setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        MetricsStateMachine.class, StateMachine.class);
  }

  static class MetricsStateMachine extends BaseStateMachine {
    static MetricsStateMachine get(RaftServerImpl s) {
      return (MetricsStateMachine)s.getStateMachine();
    }

    private final AtomicInteger flushCount = new AtomicInteger();

    int getFlushCount() {
      return flushCount.get();
    }

    @Override
    public CompletableFuture<Void> flushStateMachineData(long index) {
      flushCount.incrementAndGet();
      return super.flushStateMachineData(index);
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
        client.send(message);
      }
    }

    // For leader, flush must happen before client can get replies.
    assertFlushCount(cluster.getLeader());
    assertRaftLogWritePathMetrics(cluster.getLeader());

    // For followers, flush can be lagged behind.  Attempt multiple times.
    for(RaftServerImpl f : cluster.getFollowers()) {
      JavaUtils.attempt(() -> assertFlushCount(f), 10, HUNDRED_MILLIS, f.getId() + "-assertFlushCount", null);
      // We have already waited enough for follower metrics to populate.
      assertRaftLogWritePathMetrics(f);
    }
  }

  static void assertFlushCount(RaftServerImpl server) throws Exception {
    final String flushTimeMetric = RaftStorageTestUtils.getLogFlushTimeMetric(server.getId());
    Timer tm = (Timer) RatisMetrics.getMetricRegistryForLogWorker(server.getId().toString())
        .get(RAFT_LOG_FLUSH_TIME);
    Assert.assertNotNull(tm);

    final MetricsStateMachine stateMachine = MetricsStateMachine.get(server);
    final int expectedFlush = stateMachine.getFlushCount();

    Assert.assertEquals(expectedFlush, tm.getCount());
    Assert.assertTrue(tm.getMeanRate() > 0);

    // Test jmx
    ObjectName oname = new ObjectName("ratis_core", "name", flushTimeMetric);
    Assert.assertEquals(expectedFlush,
        ((Long) ManagementFactory.getPlatformMBeanServer().getAttribute(oname, "Count"))
            .intValue());
  }

  static void assertRaftLogWritePathMetrics(RaftServerImpl server) throws Exception {
    final String syncTimeMetric = RaftStorageTestUtils.getRaftLogFullMetric(server.getId(), RAFT_LOG_SYNC_TIME);
    RatisMetricRegistry ratisMetricRegistry = RatisMetrics.getMetricRegistryForLogWorker(server.getId().toString());

    //Test sync count
    Timer tm = (Timer) ratisMetricRegistry.get(RAFT_LOG_SYNC_TIME);
    Assert.assertNotNull(tm);
    final MetricsStateMachine stateMachine = MetricsStateMachine.get(server);
    final int expectedFlush = stateMachine.getFlushCount();
    Assert.assertEquals(expectedFlush, tm.getCount()); // Ideally, flushCount should be same as syncCount.
    Assert.assertTrue(tm.getMeanRate() > 0);

    // Test jmx. Just testing one metric's JMX is good enough.
    ObjectName oname = new ObjectName("ratis_core", "name", syncTimeMetric);
    Assert.assertEquals(expectedFlush,
        ((Long) ManagementFactory.getPlatformMBeanServer().getAttribute(oname, "Count"))
            .intValue());

    long cacheMissCount = ratisMetricRegistry.counter("cacheMissCount").getCount();
    Assert.assertTrue(cacheMissCount == 0);

    long cacheHitsCount = ratisMetricRegistry.counter("cacheHitCount").getCount();
    Assert.assertTrue(cacheHitsCount > 0);

    Timer appendLatencyTimer = ratisMetricRegistry.timer("appendEntryLatency");
    Assert.assertTrue(appendLatencyTimer.getMeanRate() > 0);

    Timer enqueuedTimer = ratisMetricRegistry.timer("enqueuedTime");
    Assert.assertTrue(enqueuedTimer.getMeanRate() > 0);

    Timer queueingDelayTimer = ratisMetricRegistry.timer("queueingDelay");
    Assert.assertTrue(queueingDelayTimer.getMeanRate() > 0);

    Timer executionTimer = ratisMetricRegistry.timer("writelogExecutionTime");
    Assert.assertTrue(executionTimer.getMeanRate() > 0);

    Assert.assertNotNull(ratisMetricRegistry.get("dataQueueSize"));
    Assert.assertNotNull(ratisMetricRegistry.get("workerQueueSize"));
    Assert.assertNotNull(ratisMetricRegistry.get("syncBatchSize"));
  }
}