/**
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

import org.apache.log4j.Level;
import org.apache.ratis.BaseTest;
import org.apache.ratis.MiniRaftCluster;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.storage.RaftStorageTestUtils;
import org.apache.ratis.util.ExitUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.LogUtils;
import org.apache.ratis.util.TimeDuration;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.apache.ratis.RaftTestUtil.waitAndKillLeader;
import static org.apache.ratis.RaftTestUtil.waitForLeader;

public abstract class LeaderElectionTests<CLUSTER extends MiniRaftCluster>
    extends BaseTest
    implements MiniRaftCluster.Factory.Get<CLUSTER> {
  {
    LogUtils.setLogLevel(RaftServerImpl.LOG, Level.DEBUG);
    LogUtils.setLogLevel(RaftClient.LOG, Level.DEBUG);
  }

  @Test
  public void testBasicLeaderElection() throws Exception {
    LOG.info("Running testBasicLeaderElection");
    final MiniRaftCluster cluster = newCluster(5);
    cluster.start();
    waitAndKillLeader(cluster, true);
    waitAndKillLeader(cluster, true);
    waitAndKillLeader(cluster, true);
    waitAndKillLeader(cluster, false);
    cluster.shutdown();
  }

  @Test
  public void testChangeLeader() throws Exception {
    RaftStorageTestUtils.setRaftLogWorkerLogLevel(Level.TRACE);
    LOG.info("Running testChangeLeader");
    final MiniRaftCluster cluster = newCluster(3);
    cluster.start();

    RaftPeerId leader = RaftTestUtil.waitForLeader(cluster).getId();
    for(int i = 0; i < 10; i++) {
      leader = RaftTestUtil.changeLeader(cluster, leader);
      ExitUtils.assertNotTerminated();
    }
    RaftStorageTestUtils.setRaftLogWorkerLogLevel(Level.INFO);
    cluster.shutdown();
  }

  @Test
  public void testEnforceLeader() throws Exception {
    final int numServer = 3;
    LOG.info("Running testEnforceLeader");
    final String leader = "s" + ThreadLocalRandom.current().nextInt(numServer);
    LOG.info("enforce leader to " + leader);
    final MiniRaftCluster cluster = newCluster(numServer);
    cluster.start();
    waitForLeader(cluster);
    waitForLeader(cluster, leader);
    cluster.shutdown();
  }

  @Test
  public void testLateServerStart() throws Exception {
    final int numServer = 3;
    LOG.info("Running testLateServerStart");
    final MiniRaftCluster cluster = newCluster(numServer);
    cluster.initServers();

    // start all except one servers
    final Iterator<RaftServerProxy> i = cluster.getServers().iterator();
    for(int j = 1; j < numServer; j++) {
      i.next().start();
    }

    final RaftServerImpl leader = waitForLeader(cluster);
    final TimeDuration sleepTime = TimeDuration.valueOf(3, TimeUnit.SECONDS);
    LOG.info("sleep " + sleepTime);
    sleepTime.sleep();

    // start the last server
    final RaftServerProxy lastServer = i.next();
    lastServer.start();
    final RaftPeerId lastServerLeaderId = JavaUtils.attempt(
        () -> getLeader(lastServer.getImpls().iterator().next().getState()),
        10, 1000, "getLeaderId", LOG);
    LOG.info(cluster.printServers());
    Assert.assertEquals(leader.getId(), lastServerLeaderId);
  }

  static RaftPeerId getLeader(ServerState state) {
    final RaftPeerId leader = state.getLeaderId();
    if (leader == null) {
      throw new IllegalStateException("No leader yet");
    }
    return leader;
  }
}
