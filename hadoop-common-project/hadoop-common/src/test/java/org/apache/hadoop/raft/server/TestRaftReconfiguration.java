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
package org.apache.hadoop.raft.server;

import org.apache.hadoop.raft.MiniRaftCluster;
import org.apache.hadoop.raft.MiniRaftCluster.PeerChanges;
import org.apache.hadoop.raft.RaftTestUtil;
import org.apache.hadoop.raft.client.RaftClient;
import org.apache.hadoop.raft.protocol.RaftPeer;
import org.apache.hadoop.raft.protocol.SetConfigurationRequest;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

public class TestRaftReconfiguration {
  static {
    GenericTestUtils.setLogLevel(RaftServer.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(RaftLog.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(RequestHandler.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(RaftClient.LOG, Level.DEBUG);
  }
  static final Logger LOG = LoggerFactory.getLogger(TestRaftReconfiguration.class);

  /**
   * add 2 new peers (3 peers -> 5 peers), no leader change
   */
  @Test
  public void testAddPeers() throws Exception {
    LOG.info("Start testAddPeers");
    MiniRaftCluster cluster =  new MiniRaftCluster(3);
    cluster.start();
    try {
      RaftTestUtil.waitForLeader(cluster);

      // add new peers
      RaftPeer[] allPeers = cluster.addNewPeers(2, true).allPeersInNewConf;

      // trigger setConfiguration
      SetConfigurationRequest request = new SetConfigurationRequest("client",
          cluster.getLeader().getId(), allPeers);
      LOG.info("Start changing the configuration: {}", request);
      cluster.getLeader().setConfiguration(request);

      // wait for the new configuration to take effect
      waitAndCheckNewConf(cluster, allPeers, 0, null);
    } finally {
      cluster.shutdown();
    }
  }

  static void waitAndCheckNewConf(MiniRaftCluster cluster, RaftPeer[] peers,
      int numOfRemovedPeers, Collection<String> deadPeers) throws Exception {
    Thread.sleep(RaftConstants.ELECTION_TIMEOUT_MAX_MS * (numOfRemovedPeers + 2));
    LOG.info(cluster.printServers());
    Assert.assertNotNull(cluster.getLeader());

    int numIncluded = 0;
    int deadIncluded = 0;
    RaftConfiguration current = new RaftConfiguration(peers, 0);
    for (RaftServer server : cluster.getServers()) {
      if (deadPeers != null && deadPeers.contains(server.getId())) {
        if (current.containsInConf(server.getId())) {
          deadIncluded++;
        }
        continue;
      }
      if (current.containsInConf(server.getId())) {
        numIncluded++;
        Assert.assertTrue(server.getRaftConf().inStableState());
        Assert.assertTrue(server.getRaftConf().hasNoChange(peers));
      } else {
        Assert.assertFalse(server.getId() + " is still running: " + server,
            server.isRunning());
      }
    }
    Assert.assertEquals(peers.length, numIncluded + deadIncluded);
  }

  /**
   * remove 2 peers (5 peers -> 3 peers), no leader change
   */
  @Test
  public void testRemovePeers() throws Exception {
    LOG.info("Start testRemovePeers");
    MiniRaftCluster cluster =  new MiniRaftCluster(5);
    cluster.start();
    try {
      RaftTestUtil.waitForLeader(cluster);

      // remove peers, leader still included in the new conf
      RaftPeer[] allPeers = cluster
          .removePeers(2, false, Collections.emptyList()).allPeersInNewConf;

      // trigger setConfiguration
      SetConfigurationRequest request = new SetConfigurationRequest("client",
          cluster.getLeader().getId(), allPeers);
      LOG.info("Start changing the configuration: {}", request);
      cluster.getLeader().setConfiguration(request);

      // wait for the new configuration to take effect
      waitAndCheckNewConf(cluster, allPeers, 2, null);
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * 5 peers -> 5 peers, remove 2 old, add 2 new, no leader change
   */
  @Test
  public void testAddRemovePeers() throws Exception {
    LOG.info("Start testAddRemovePeers");
    testAddRemovePeers(false);
  }

  @Test
  public void testLeaderStepDown() throws Exception {
    LOG.info("Start testLeaderStepDown");
    testAddRemovePeers(true);
  }

  private void testAddRemovePeers(boolean leaderStepdown) throws Exception {
    MiniRaftCluster cluster =  new MiniRaftCluster(5);
    cluster.start();
    try {
      RaftTestUtil.waitForLeader(cluster);

      PeerChanges change = cluster.addNewPeers(2, true);
      RaftPeer[] allPeers = cluster.removePeers(2, leaderStepdown,
          Arrays.asList(change.newPeers)).allPeersInNewConf;

      // trigger setConfiguration
      SetConfigurationRequest request = new SetConfigurationRequest("client",
          cluster.getLeader().getId(), allPeers);
      LOG.info("Start changing the configuration: {}", request);
      cluster.getLeader().setConfiguration(request);

      // wait for the new configuration to take effect
      waitAndCheckNewConf(cluster, allPeers, 2, null);
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testKillLeaderDuringReconf() throws Exception {
    LOG.info("Start testKillLeaderDuringReconf");
    // originally 3 peers
    MiniRaftCluster cluster =  new MiniRaftCluster(3);
    cluster.start();
    try {
      RaftTestUtil.waitForLeader(cluster);

      PeerChanges c1 = cluster.addNewPeers(2, false);
      PeerChanges c2 = cluster.removePeers(2, false, Arrays.asList(c1.newPeers));

      SetConfigurationRequest request = new SetConfigurationRequest("client",
          cluster.getLeader().getId(), c2.allPeersInNewConf);
      LOG.info("Start changing the configuration: {}", request);
      cluster.getLeader().setConfiguration(request);

      // the leader cannot commit the (old, new) conf since the two 2 peers have
      // not started yet
      LOG.info(cluster.printServers());
      Assert.assertTrue(cluster.getLeader().getRaftConf().inTransitionState());
      // only the first empty entry got committed
      final long committedIndex = cluster.getLeader().getState().getLog()
          .getLastCommitted().getIndex();
      Assert.assertTrue("committedIndex is " + committedIndex,
          committedIndex <= 1);

      // kill the current leader
      final String oldLeaderId = RaftTestUtil.waitAndKillLeader(cluster, true);
      // start the two new peers
      for (RaftPeer np : c1.newPeers) {
        cluster.startServer(np.getId());
      }

      waitAndCheckNewConf(cluster, c2.allPeersInNewConf, 2,
          Collections.singletonList(oldLeaderId));
    } finally {
      cluster.shutdown();
    }
  }
}
