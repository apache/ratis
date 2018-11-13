/**
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

package org.apache.ratis.server;

import com.codahale.metrics.Timer;
import org.apache.log4j.Level;
import org.apache.ratis.BaseTest;
import org.apache.ratis.MiniRaftCluster;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.metrics.RatisMetricsRegistry;
import org.apache.ratis.server.impl.RaftServerImpl;
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
  public void testFlushMetric() throws Exception {
    try(final MiniRaftCluster cluster = newCluster(NUM_SERVERS)) {
      cluster.start();
      runTestFlushMetric(cluster);
    }
  }

  static void runTestFlushMetric(MiniRaftCluster cluster) throws Exception {
    int numMsg = 2;
    final RaftTestUtil.SimpleMessage[] messages = RaftTestUtil.SimpleMessage.create(numMsg);

    try (final RaftClient client = cluster.createClient()) {
      for (RaftTestUtil.SimpleMessage message : messages) {
        client.send(message);
      }
    }

    // For leader, flush must happen before client can get replies.
    assertFlushCount(cluster.getLeader());

    // For followers, flush can be lagged behind.  Attempt multiple times.
    for(RaftServerImpl f : cluster.getFollowers()) {
      JavaUtils.attempt(() -> assertFlushCount(f), 10, 100, f.getId() + "-assertFlushCount", null);
    }
  }

  static void assertFlushCount(RaftServerImpl server) throws Exception {
      final String flushTimeMetric = RaftStorageTestUtils.getLogFlushTimeMetric(server.getId());
      Timer tm = RatisMetricsRegistry.getRegistry().getTimers().get(flushTimeMetric);
      Assert.assertNotNull(tm);

      final MetricsStateMachine stateMachine = MetricsStateMachine.get(server);
      final int expectedFlush = stateMachine.getFlushCount();

      Assert.assertEquals(expectedFlush, tm.getCount());
      Assert.assertTrue(tm.getMeanRate() > 0);

      // Test jmx
      ObjectName oname = new ObjectName("metrics", "name", flushTimeMetric);
      Assert.assertEquals(expectedFlush,
          ((Long) ManagementFactory.getPlatformMBeanServer().getAttribute(oname, "Count"))
              .intValue());
  }
}
