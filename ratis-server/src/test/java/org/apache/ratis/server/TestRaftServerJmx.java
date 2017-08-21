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

import org.apache.log4j.Level;
import org.apache.ratis.BaseTest;
import org.apache.ratis.MiniRaftCluster;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.simulation.MiniRaftClusterWithSimulatedRpc;
import org.apache.ratis.util.LogUtils;
import org.junit.*;

import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Set;

import static org.apache.ratis.RaftTestUtil.waitForLeader;

public class TestRaftServerJmx extends BaseTest {
  static {
    LogUtils.setLogLevel(RaftServerImpl.LOG, Level.DEBUG);
    LogUtils.setLogLevel(RaftClient.LOG, Level.DEBUG);
  }

  public static final int NUM_SERVERS = 5;

  protected static final RaftProperties properties = new RaftProperties();

  private final MiniRaftClusterWithSimulatedRpc cluster;

  public TestRaftServerJmx() throws IOException {
    cluster = MiniRaftClusterWithSimulatedRpc.FACTORY.newCluster(
        NUM_SERVERS, getProperties());
  }

  public RaftProperties getProperties() {
    return properties;
  }

  @Before
  public void setup() throws IOException {
    Assert.assertNull(getCluster().getLeader());
    getCluster().start();
  }

  @After
  public void tearDown() {
    final MiniRaftCluster cluster = getCluster();
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  public MiniRaftClusterWithSimulatedRpc getCluster() {
    return cluster;
  }

  @Test
  public void testJmxBeans() throws Exception {
    RaftServerImpl leader = waitForLeader(cluster);
    System.out.println(cluster.getLeader());
    MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
    Set<ObjectInstance> objectInstances = platformMBeanServer.queryMBeans(new ObjectName("Ratis:*"), null);
    Assert.assertEquals(NUM_SERVERS, objectInstances.size());

    for (ObjectInstance instance : objectInstances) {
      Object groupId = platformMBeanServer.getAttribute(instance.getObjectName(), "GroupId");
      Assert.assertEquals(cluster.getGroupId().toString(), groupId);
    }
  }

}
