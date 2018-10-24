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
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.metrics.RatisMetricsRegistry;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.impl.RaftServerProxy;
import org.apache.ratis.server.simulation.MiniRaftClusterWithSimulatedRpc;
import org.apache.ratis.util.LogUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.management.ObjectName;
import java.io.IOException;
import java.lang.management.ManagementFactory;

public class TestRaftLogMetrics {

  {
    LogUtils.setLogLevel(RaftServerImpl.LOG, Level.DEBUG);
    LogUtils.setLogLevel(RaftClient.LOG, Level.DEBUG);
  }

  public static final int NUM_SERVERS = 3;

  protected static final RaftProperties properties = new RaftProperties();

  private final MiniRaftClusterWithSimulatedRpc cluster = MiniRaftClusterWithSimulatedRpc
      .FACTORY.newCluster(NUM_SERVERS, getProperties());

  public RaftProperties getProperties() {
    return properties;
  }

  @Before
  public void setup() throws IOException {
    Assert.assertNull(cluster.getLeader());
    cluster.start();
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private String getLogFlushTimeMetric(String serverId) {
    return new StringBuilder("org.apache.ratis.server.storage.RaftLogWorker.")
        .append(serverId).append(".flush-time").toString();
  }

  @Test
  public void testFlushMetric() throws Exception {
    int numMsg = 2;
    final RaftTestUtil.SimpleMessage[] messages = RaftTestUtil.SimpleMessage.create(numMsg);

    try (final RaftClient client = cluster.createClient()) {
      for (RaftTestUtil.SimpleMessage message : messages) {
        client.send(message);
      }
    }

    for (RaftServerProxy rsp: cluster.getServers()) {
      String flushTimeMetric = getLogFlushTimeMetric(rsp.getId().toString());
      Timer tm = RatisMetricsRegistry.getRegistry().getTimers().get(flushTimeMetric);
      Assert.assertNotNull(tm);

      // Number of log entries expected = numMsg + 1 entry for start-log-segment
      int numExpectedLogEntries = numMsg + 1;

      Assert.assertEquals(numExpectedLogEntries, tm.getCount());
      Assert.assertTrue(tm.getMeanRate() > 0);

      // Test jmx
      ObjectName oname = new ObjectName("metrics", "name", flushTimeMetric);
      Assert.assertEquals(numExpectedLogEntries,
          ((Long) ManagementFactory.getPlatformMBeanServer().getAttribute(oname, "Count"))
              .intValue());
    }
  }

}
