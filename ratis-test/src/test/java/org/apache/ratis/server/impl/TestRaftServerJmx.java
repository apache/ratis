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
package org.apache.ratis.server.impl;

import org.apache.ratis.BaseTest;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServerMXBean;
import org.apache.ratis.server.simulation.MiniRaftClusterWithSimulatedRpc;
import org.apache.ratis.util.JmxRegister;
import org.junit.Assert;
import org.junit.Test;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Set;

import static org.apache.ratis.RaftTestUtil.waitForLeader;

public class TestRaftServerJmx extends BaseTest {
  @Test(timeout = 30000)
  public void testJmxBeans() throws Exception {
    final int NUM_SERVERS = 3;
    final MiniRaftClusterWithSimulatedRpc cluster
        = MiniRaftClusterWithSimulatedRpc.FACTORY.newCluster(3, new RaftProperties());
    cluster.start();
    waitForLeader(cluster);

    MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
    Set<ObjectInstance> objectInstances = platformMBeanServer.queryMBeans(new ObjectName("Ratis:*"), null);
    Assert.assertEquals(NUM_SERVERS, objectInstances.size());

    for (ObjectInstance instance : objectInstances) {
      Object groupId = platformMBeanServer.getAttribute(instance.getObjectName(), "GroupId");
      Assert.assertEquals(cluster.getGroupId().toString(), groupId);
    }
    cluster.shutdown();
  }

  @Test(timeout = 30000)
  public void testRegister() throws JMException {
    {
      final JmxRegister jmx = new JmxRegister();
      runUnregister(false, jmx);

      runRegister(true, "abc", jmx);
      runRegister(false, "abc", jmx);
      runUnregister(true, jmx);
      runUnregister(false, jmx);

      runRegister(true, "abc", jmx);
      runUnregister(true, jmx);
      runUnregister(false, jmx);
    }

    {
      final JmxRegister jmx = new JmxRegister();
      runRegister(true, "host:1234", jmx);
      runUnregister(true, jmx);
      runUnregister(false, jmx);
    }
  }

  static void runRegister(boolean expectToSucceed, String name, JmxRegister jmx) {
    final RaftServerMXBean mBean = new RaftServerMXBean() {
      @Override
      public String getId() { return null; }
      @Override
      public String getLeaderId() { return null; }
      @Override
      public long getCurrentTerm() { return 0; }
      @Override
      public String getGroupId() { return null; }
      @Override
      public String getRole() { return null; }
      @Override
      public List<String> getFollowers() { return null; }
      @Override
      public List<String> getGroups() { return null; }

    };
    final RaftPeerId id = RaftPeerId.valueOf(name);
    final RaftGroupId groupId = RaftGroupId.randomId();
    final boolean succeeded = RaftServerImpl.registerMBean(id, groupId, mBean, jmx);
    Assert.assertEquals(expectToSucceed, succeeded);
  }

  static void runUnregister(boolean expectToSucceed, JmxRegister jmx) throws JMException {
    final boolean succeeded = jmx.unregister();
    Assert.assertEquals(expectToSucceed, succeeded);
  }
}
