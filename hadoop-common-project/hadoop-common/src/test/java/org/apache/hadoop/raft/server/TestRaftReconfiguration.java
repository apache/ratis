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
import org.apache.hadoop.raft.server.simulation.MiniRaftClusterWithSimulatedRpc;
import org.apache.hadoop.raft.RaftTestUtil;
import org.apache.hadoop.raft.RaftTestUtil.SimpleMessage;
import org.apache.hadoop.raft.client.RaftClient;
import org.apache.hadoop.raft.conf.RaftProperties;
import org.apache.hadoop.raft.protocol.RaftClientReply;
import org.apache.hadoop.raft.protocol.RaftPeer;
import org.apache.hadoop.raft.protocol.SetConfigurationRequest;
import org.apache.hadoop.raft.server.simulation.RequestHandler;
import org.apache.hadoop.raft.server.storage.MemoryRaftLog;
import org.apache.hadoop.raft.server.storage.RaftLog;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Arrays.asList;

public class TestRaftReconfiguration {
  static {
    GenericTestUtils.setLogLevel(RaftServer.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(MemoryRaftLog.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(RequestHandler.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(RaftClient.LOG, Level.DEBUG);
  }
  static final Logger LOG = LoggerFactory.getLogger(TestRaftReconfiguration.class);

  private final RaftProperties prop = new RaftProperties();

  @Before
  public void setup() {
    prop.setBoolean(RaftServerConfigKeys.RAFT_SERVER_USE_MEMORY_LOG_KEY, false);
  }

  /**
   * add 2 new peers (3 peers -> 5 peers), no leader change
   */
  @Test
  public void testAddPeers() throws Exception {
    LOG.info("Start testAddPeers");
    MiniRaftCluster cluster =  new MiniRaftClusterWithSimulatedRpc(3, prop);
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
    RaftConfiguration current = RaftConfiguration.composeConf(peers, 0);
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
    MiniRaftCluster cluster =  new MiniRaftClusterWithSimulatedRpc(5, prop);
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
    MiniRaftCluster cluster =  new MiniRaftClusterWithSimulatedRpc(5, prop);
    cluster.start();
    try {
      RaftTestUtil.waitForLeader(cluster);

      PeerChanges change = cluster.addNewPeers(2, true);
      RaftPeer[] allPeers = cluster.removePeers(2, leaderStepdown,
          asList(change.newPeers)).allPeersInNewConf;

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

  @Test(timeout = 30000)
  public void testReconfTwice() throws Exception {
    LOG.info("Start testReconfTwice");
    final MiniRaftCluster cluster = new MiniRaftClusterWithSimulatedRpc(3, prop);
    cluster.start();
    try {
      RaftTestUtil.waitForLeader(cluster);
      final String leaderId = cluster.getLeader().getId();
      final RaftClient client = cluster.createClient("client", leaderId);

      // submit some msgs before reconf
      for (int i = 0; i < RaftConstants.STAGING_CATCHUP_GAP * 2; i++) {
        RaftClientReply reply = client.send(new SimpleMessage("m" + i));
        Assert.assertTrue(reply.isSuccess());
      }

      final AtomicBoolean reconf1 = new AtomicBoolean(false);
      final AtomicBoolean reconf2 = new AtomicBoolean(false);
      final AtomicReference<RaftPeer[]> finalPeers = new AtomicReference<>(null);
      final AtomicReference<RaftPeer[]> deadPeers = new AtomicReference<>(null);
      CountDownLatch latch = new CountDownLatch(1);
      Thread clientThread = new Thread(() -> {
        try {
          PeerChanges c1 = cluster.addNewPeers(2, true);
          LOG.info("Start changing the configuration: {}",
              asList(c1.allPeersInNewConf));

          RaftClientReply reply = client.setConfiguration(c1.allPeersInNewConf);
          reconf1.set(reply.isSuccess());

          PeerChanges c2 = cluster.removePeers(2, true, asList(c1.newPeers));
          finalPeers.set(c2.allPeersInNewConf);
          deadPeers.set(c2.removedPeers);
          LOG.info("Start changing the configuration again: {}",
              asList(c2.allPeersInNewConf));
          reply = client.setConfiguration(c2.allPeersInNewConf);
          reconf2.set(reply.isSuccess());

          latch.countDown();
        } catch (IOException ignored) {
        }
      });
      clientThread.start();

      latch.await();
      Assert.assertTrue(reconf1.get());
      Assert.assertTrue(reconf2.get());
      waitAndCheckNewConf(cluster, finalPeers.get(), 2, null);

      // check configuration manager's internal state
      // each reconf will generate two configurations: (old, new) and (new)
      cluster.getServers().stream().filter(RaftServer::isRunning)
          .forEach(server -> {
        ConfigurationManager confManager =
            (ConfigurationManager) Whitebox.getInternalState(server.getState(),
                "configurationManager");
        // each reconf will generate two configurations: (old, new) and (new)
        Assert.assertEquals(5, confManager.numOfConf());
      });
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testReconfTimeout() throws Exception {
    LOG.info("Start testReconfTimeout");
    // originally 3 peers
    final MiniRaftCluster cluster = new MiniRaftClusterWithSimulatedRpc(3, prop);
    cluster.start();
    try {
      RaftTestUtil.waitForLeader(cluster);
      final String leaderId = cluster.getLeader().getId();
      final RaftClient client = cluster.createClient("client", leaderId);

      PeerChanges c1 = cluster.addNewPeers(2, false);

      LOG.info("Start changing the configuration: {}",
          asList(c1.allPeersInNewConf));
      final AtomicBoolean success = new AtomicBoolean(false);

      CountDownLatch latch = new CountDownLatch(1);
      Thread clientThread = new Thread(() -> {
        try {
          latch.countDown();
          RaftClientReply reply = client.setConfiguration(c1.allPeersInNewConf);
          success.set(reply.isSuccess());
        } catch (IOException ignored) {
        }
      });
      clientThread.start();

      Assert.assertFalse(cluster.getLeader().getRaftConf().inTransitionState());
      latch.await();
      Thread.sleep(RaftConstants.STAGING_NOPROGRESS_TIMEOUT + 200);

      // the two new peers have not started yet, the bootstrapping must timeout
      Assert.assertFalse(success.get());
      LOG.info(cluster.printServers());
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testBootstrapReconf() throws Exception {
    LOG.info("Start testBootstrapReconf");
    // originally 3 peers
    final MiniRaftCluster cluster =  new MiniRaftClusterWithSimulatedRpc(3, prop);
    cluster.start();
    try {
      RaftTestUtil.waitForLeader(cluster);
      final String leaderId = cluster.getLeader().getId();
      final RaftClient client = cluster.createClient("client", leaderId);

      // submit some msgs before reconf
      for (int i = 0; i < RaftConstants.STAGING_CATCHUP_GAP * 2; i++) {
        RaftClientReply reply = client.send(new SimpleMessage("m" + i));
        Assert.assertTrue(reply.isSuccess());
      }

      PeerChanges c1 = cluster.addNewPeers(2, true);
      LOG.info("Start changing the configuration: {}",
          asList(c1.allPeersInNewConf));
      final AtomicReference<Boolean> success = new AtomicReference<>();

      Thread clientThread = new Thread(() -> {
        try {
          RaftClientReply reply = client.setConfiguration(c1.allPeersInNewConf);
          success.set(reply.isSuccess());
        } catch (IOException ioe) {
          LOG.error("FAILED", ioe);
        }
      });
      clientThread.start();

      Thread.sleep(5000);
      LOG.info(cluster.printServers());
      assertSuccess(success);

      final RaftLog leaderLog = cluster.getLeader().getState().getLog();
      for (RaftPeer newPeer : c1.newPeers) {
        Assert.assertArrayEquals(leaderLog.getEntries(0, Long.MAX_VALUE),
            cluster.getServer(newPeer.getId()).getState().getLog()
                .getEntries(0, Long.MAX_VALUE));
      }
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * kill the leader before reconfiguration finishes. Make sure the client keeps
   * retrying.
   */
  @Test
  public void testKillLeaderDuringReconf() throws Exception {
    LOG.info("Start testKillLeaderDuringReconf");
    // originally 3 peers
    final MiniRaftCluster cluster =  new MiniRaftClusterWithSimulatedRpc(3, prop);
    cluster.start();
    try {
      RaftTestUtil.waitForLeader(cluster);
      final String leaderId = cluster.getLeader().getId();
      final RaftClient client = cluster.createClient("client", leaderId);

      PeerChanges c1 = cluster.addNewPeers(2, false);
      PeerChanges c2 = cluster.removePeers(2, false, asList(c1.newPeers));

      LOG.info("Start changing the configuration: {}",
          asList(c2.allPeersInNewConf));
      final AtomicReference<Boolean> success = new AtomicReference<>();
      final AtomicBoolean clientRunning = new AtomicBoolean(true);
      Thread clientThread = new Thread(() -> {
        try {
          boolean r = false;
          while (clientRunning.get() && !r) {
            r = client.setConfiguration(c2.allPeersInNewConf).isSuccess();
          }
          success.set(r);
        } catch (IOException ignored) {
        }
      });
      clientThread.start();

      // the leader cannot generate the (old, new) conf, and it will keep
      // bootstrapping the 2 new peers since they have not started yet
      LOG.info(cluster.printServers());
      Assert.assertFalse(cluster.getLeader().getRaftConf().inTransitionState());

      // only the first empty entry got committed
      final long committedIndex = cluster.getLeader().getState().getLog()
          .getLastCommitted().getIndex();
      Assert.assertTrue("committedIndex is " + committedIndex,
          committedIndex <= 1);

      // kill the current leader
      final String oldLeaderId = RaftTestUtil.waitAndKillLeader(cluster, true);
      // start the two new peers
      for (RaftPeer np : c1.newPeers) {
        cluster.startServer(np.getId(), null);
      }

      Thread.sleep(3000);
      // the client should get the NotLeaderException from the first leader, and
      // will retry the same setConfiguration request
      waitAndCheckNewConf(cluster, c2.allPeersInNewConf, 2,
          Collections.singletonList(oldLeaderId));
      clientRunning.set(false);
      //Assert.assertTrue(success.get());
    } finally {
      cluster.shutdown();
    }
  }

  static void assertSuccess(final AtomicReference<Boolean> success) {
    final String s = "success=" + success;
    Assert.assertNotNull(s, success.get());
    Assert.assertTrue(s, success.get());
  }

  // TODO: raft log inconsistency between leader and follower

  // TODO: follower truncates its log entries which causes configuration change
}
