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
package org.apache.hadoop.raft;

import org.apache.hadoop.raft.client.RaftClient;
import org.apache.hadoop.raft.protocol.RaftPeer;
import org.apache.hadoop.raft.protocol.SetConfigurationRequest;
import org.apache.hadoop.raft.server.RaftConfiguration;
import org.apache.hadoop.raft.server.RaftConstants;
import org.apache.hadoop.raft.server.RaftLog;
import org.apache.hadoop.raft.server.RaftServer;
import org.apache.hadoop.raft.server.RequestHandler;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;

public class TestRaftReconfiguration {
  static {
    GenericTestUtils.setLogLevel(RaftServer.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(RaftLog.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(RequestHandler.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(RaftClient.LOG, Level.DEBUG);
  }
  static final Logger LOG = LoggerFactory.getLogger(TestRaftReconfiguration.class);
  static final PrintStream out = System.out;

  /**
   * add 2 new peers (3 peers -> 5 peers), no leader change
   */
  @Test
  public void testAddPeers() throws Exception {
    MiniRaftCluster cluster =  new MiniRaftCluster(3);
    cluster.start();
    RaftTestUtil.waitForLeader(cluster);

    // add new peers
    RaftPeer[] allPeers = cluster.addNewPeers(2);

    // trigger setConfiguration
    SetConfigurationRequest request = new SetConfigurationRequest("client",
        cluster.getLeader().getId(), allPeers);
    LOG.info("Start changing the configuration: {}", request);
    cluster.getLeader().setConfiguration(request);

    // wait for the new configuration to take effect
    waitAndCheckNewConf(cluster, allPeers, 0);
  }

  static void waitAndCheckNewConf(MiniRaftCluster cluster, RaftPeer[] peers,
      int numOfRemovedPeers) throws Exception {
    Thread.sleep(RaftConstants.ELECTION_TIMEOUT_MAX_MS * numOfRemovedPeers);
    cluster.printServers(out);
    Assert.assertNotNull(cluster.getLeader());

    // send a new message to commit a new entry in new term
    cluster.createClient("client", null)
        .send(new RaftTestUtil.SimpleMessage("m"));
    Thread.sleep(RaftConstants.ELECTION_TIMEOUT_MAX_MS);

    int numIncluded = 0;
    RaftConfiguration current = new RaftConfiguration(peers, 0);
    for (RaftServer server : cluster.getServers()) {
      if (current.containsInConf(server.getId())) {
        numIncluded++;
        Assert.assertTrue(server.getRaftConf().inStableState());
        Assert.assertTrue(server.getRaftConf().hasNoChange(peers));
      } else {
        Assert.assertFalse(server.isRunning());
      }
    }
    Assert.assertEquals(peers.length, numIncluded);
  }

  /**
   * remove 2 peers (5 peers -> 3 peers), no leader change
   */
  @Test
  public void testRemovePeers() throws Exception {
    MiniRaftCluster cluster =  new MiniRaftCluster(5);
    cluster.start();
    RaftTestUtil.waitForLeader(cluster);

    // remove peers, leader still included in the new conf
    RaftPeer[] allPeers = cluster.removePeers(2, false);

    // trigger setConfiguration
    SetConfigurationRequest request = new SetConfigurationRequest("client",
        cluster.getLeader().getId(), allPeers);
    LOG.info("Start changing the configuration: {}", request);
    cluster.getLeader().setConfiguration(request);

    // wait for the new configuration to take effect
    waitAndCheckNewConf(cluster, allPeers, 2);
  }

  /**
   * 5 peers -> 5 peers, remove 2 old, add 2 new, no leader change
   */
  @Test
  public void testAddRemovePeers() throws Exception {
    testAddRemovePeers(false);
  }

  @Test
  public void testLeaderStepDown() throws Exception {
    testAddRemovePeers(true);
  }

  private void testAddRemovePeers(boolean leaderStepdown) throws Exception {
    MiniRaftCluster cluster =  new MiniRaftCluster(5);
    cluster.start();
    RaftTestUtil.waitForLeader(cluster);

    cluster.addNewPeers(2);
    RaftPeer[] allPeers = cluster.removePeers(2, leaderStepdown);

    // trigger setConfiguration
    SetConfigurationRequest request = new SetConfigurationRequest("client",
        cluster.getLeader().getId(), allPeers);
    LOG.info("Start changing the configuration: {}", request);
    cluster.getLeader().setConfiguration(request);

    // wait for the new configuration to take effect
    waitAndCheckNewConf(cluster, allPeers, 2);
  }
}
