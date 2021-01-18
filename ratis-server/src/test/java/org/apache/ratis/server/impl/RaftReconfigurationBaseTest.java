/*
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
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.RaftTestUtil.SimpleMessage;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientRpc;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.SetConfigurationRequest;
import org.apache.ratis.protocol.exceptions.LeaderNotReadyException;
import org.apache.ratis.protocol.exceptions.ReconfigurationInProgressException;
import org.apache.ratis.protocol.exceptions.ReconfigurationTimeoutException;
import org.apache.ratis.server.RaftConfiguration;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.MiniRaftCluster.PeerChanges;
import org.apache.ratis.server.raftlog.LogProtoUtils;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.raftlog.RaftLogBase;
import org.apache.ratis.server.storage.RaftStorageTestUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Log4jUtils;
import org.apache.ratis.util.TimeDuration;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Arrays.asList;
import static org.apache.ratis.server.impl.RaftServerTestUtil.waitAndCheckNewConf;

public abstract class RaftReconfigurationBaseTest<CLUSTER extends MiniRaftCluster>
    extends BaseTest
    implements MiniRaftCluster.Factory.Get<CLUSTER> {
  static {
    Log4jUtils.setLogLevel(RaftServer.Division.LOG, Level.DEBUG);
  }

  private static final DelayLocalExecutionInjection logSyncDelay = RaftServerTestUtil.getLogSyncDelay();
  private static final DelayLocalExecutionInjection leaderPlaceHolderDelay =
      new DelayLocalExecutionInjection(LeaderStateImpl.APPEND_PLACEHOLDER);

  static final int STAGING_CATCHUP_GAP = 10;

  {
    RaftServerConfigKeys.setStagingCatchupGap(getProperties(), STAGING_CATCHUP_GAP);
  }

  private void checkPriority(CLUSTER cluster, RaftGroupId groupId, List<RaftPeer> peersWithPriority)
      throws InterruptedException {
    RaftTestUtil.waitForLeader(cluster, groupId);

    for (int i = 0; i < peersWithPriority.size(); i ++) {
      RaftPeerId peerId = peersWithPriority.get(i).getId();
      final RaftServer.Division server = cluster.getDivision(peerId, groupId);
      final RaftConfiguration conf = server.getRaftConf();

      for (int j = 0; j < peersWithPriority.size(); j ++) {
        int priorityInConf = conf.getPeer(peersWithPriority.get(j).getId()).getPriority();
        Assert.assertEquals(priorityInConf, peersWithPriority.get(j).getPriority());
      }
    }
  }

  @Test
  public void testRestorePriority() throws Exception {
    runWithNewCluster(3, cluster -> {
      // Add groups
      List<RaftPeer> peers = cluster.getPeers();

      List<RaftPeer> peersWithPriority = new ArrayList<>();
      for (int i = 0; i < peers.size(); i++) {
        RaftPeer peer = peers.get(i);
        peersWithPriority.add(
            RaftPeer.newBuilder(peer).setPriority(i).build());
      }

      final RaftGroup newGroup = RaftGroup.valueOf(RaftGroupId.randomId(), peersWithPriority);
      LOG.info("add new group: " + newGroup);
      try (final RaftClient client = cluster.createClient(newGroup)) {
        for (RaftPeer p : newGroup.getPeers()) {
          client.getGroupManagementApi(p.getId()).add(newGroup);
        }
      }

      RaftGroupId groupId = newGroup.getGroupId();

      checkPriority(cluster, groupId, peersWithPriority);

      cluster.restart(false);

      checkPriority(cluster, groupId, peersWithPriority);
    });
  }

  /**
   * add 2 new peers (3 peers -> 5 peers), no leader change
   */
  @Test
  public void testAddPeers() throws Exception {
    runWithNewCluster(3, cluster -> {
      RaftTestUtil.waitForLeader(cluster);

      // add new peers
      RaftPeer[] allPeers = cluster.addNewPeers(2, true).allPeersInNewConf;

      // trigger setConfiguration
      cluster.setConfiguration(allPeers);

      // wait for the new configuration to take effect
      waitAndCheckNewConf(cluster, allPeers, 0, null);
    });
  }

  /**
   * remove 2 peers (5 peers -> 3 peers), no leader change
   */
  @Test
  public void testRemovePeers() throws Exception {
    runWithNewCluster(5, cluster -> {
      RaftTestUtil.waitForLeader(cluster);

      // remove peers, leader still included in the new conf
      RaftPeer[] allPeers = cluster
          .removePeers(2, false, Collections.emptyList()).allPeersInNewConf;

      // trigger setConfiguration
      cluster.setConfiguration(allPeers);

      // wait for the new configuration to take effect
      waitAndCheckNewConf(cluster, allPeers, 2, null);
    });
  }

  /**
   * 5 peers -> 5 peers, remove 2 old, add 2 new, no leader change
   */
  @Test
  public void testAddRemovePeers() throws Exception {
    runWithNewCluster(5, cluster -> runTestAddRemovePeers(false, cluster));
  }

  @Test
  public void testLeaderStepDown() throws Exception {
    runWithNewCluster(5, cluster -> runTestAddRemovePeers(true, cluster));
  }

  private void runTestAddRemovePeers(boolean leaderStepdown, CLUSTER cluster) throws Exception {
      RaftTestUtil.waitForLeader(cluster);

      PeerChanges change = cluster.addNewPeers(2, true);
      RaftPeer[] allPeers = cluster.removePeers(2, leaderStepdown,
          asList(change.newPeers)).allPeersInNewConf;

      // trigger setConfiguration
      cluster.setConfiguration(allPeers);

      // wait for the new configuration to take effect
      waitAndCheckNewConf(cluster, allPeers, 2, null);
  }

  @Test(timeout = 30000)
  public void testReconfTwice() throws Exception {
    runWithNewCluster(3, this::runTestReconfTwice);
  }

  void runTestReconfTwice(CLUSTER cluster) throws Exception {
      final RaftPeerId leaderId = RaftTestUtil.waitForLeader(cluster).getId();
      try (final RaftClient client = cluster.createClient(leaderId)) {

        // submit some msgs before reconf
        for (int i = 0; i < STAGING_CATCHUP_GAP * 2; i++) {
          RaftClientReply reply = client.io().send(new SimpleMessage("m" + i));
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

            RaftClientReply reply = client.admin().setConfiguration(c1.allPeersInNewConf);
            reconf1.set(reply.isSuccess());

            PeerChanges c2 = cluster.removePeers(2, true, asList(c1.newPeers));
            finalPeers.set(c2.allPeersInNewConf);
            deadPeers.set(c2.removedPeers);

            LOG.info("Start changing the configuration again: {}",
                    asList(c2.allPeersInNewConf));
            reply = client.admin().setConfiguration(c2.allPeersInNewConf);
            reconf2.set(reply.isSuccess());

            latch.countDown();
          } catch (Exception ignored) {
            LOG.warn("{} is ignored", JavaUtils.getClassSimpleName(ignored.getClass()), ignored);
          }
        });
        clientThread.start();

        latch.await();
        Assert.assertTrue(reconf1.get());
        Assert.assertTrue(reconf2.get());
        waitAndCheckNewConf(cluster, finalPeers.get(), 2, null);
        final RaftPeerId leader2 = RaftTestUtil.waitForLeader(cluster).getId();

        // check configuration manager's internal state
        // each reconf will generate two configurations: (old, new) and (new)
        cluster.getServerAliveStream().forEach(server -> {
          final ConfigurationManager confManager = RaftServerTestUtil.getConfigurationManager(server);
          // each reconf will generate two configurations: (old, new) and (new)
          // each leader change generates one configuration.
          // expectedConf = 1 (init) + 2*2 (two conf changes) + #leader
          final int expectedConf = leader2.equals(leaderId) ? 6 : 7;
          Assert.assertEquals(server.getId() + ": " + confManager, expectedConf, confManager.numOfConf());
        });
      }
  }

  @Test
  public void testReconfTimeout() throws Exception {
    // originally 3 peers
    runWithNewCluster(3, this::runTestReconfTimeout);
  }

  void runTestReconfTimeout(CLUSTER cluster) throws Exception {
    final RaftPeerId leaderId = RaftTestUtil.waitForLeader(cluster).getId();
    try (final RaftClient client = cluster.createClient(leaderId)) {
      PeerChanges c1 = cluster.addNewPeers(2, false);

      LOG.info("Start changing the configuration: {}",
          asList(c1.allPeersInNewConf));
      Assert.assertFalse(((RaftConfigurationImpl)cluster.getLeader().getRaftConf()).isTransitional());

      final RaftClientRpc sender = client.getClientRpc();
      final SetConfigurationRequest request = cluster.newSetConfigurationRequest(
          client.getId(), leaderId, c1.allPeersInNewConf);
      try {
        sender.sendRequest(request);
        Assert.fail("did not get expected exception");
      } catch (IOException e) {
        Assert.assertTrue("Got exception " + e,
            e instanceof ReconfigurationTimeoutException);
      }

      // the two new peers have not started yet, the bootstrapping must timeout
      LOG.info(cluster.printServers());

      // resend the same request, make sure the server has correctly reset its
      // state so that we still get timeout instead of in-progress exception
      try {
        sender.sendRequest(request);
        Assert.fail("did not get expected exception");
      } catch (IOException e) {
        Assert.assertTrue("Got exception " + e,
            e instanceof ReconfigurationTimeoutException);
      }

      // start the two new peers
      LOG.info("Start new peers");
      for (RaftPeer np : c1.newPeers) {
        cluster.restartServer(np.getId(), false);
      }
      Assert.assertTrue(client.admin().setConfiguration(c1.allPeersInNewConf).isSuccess());
    }
  }

  @Test
  public void testBootstrapReconfWithSingleNodeAddOne() throws Exception {
    // originally 1 peer, add 1 more
    runWithNewCluster(1, cluster -> runTestBootstrapReconf(1, false, cluster));
  }

  @Test
  public void testBootstrapReconfWithSingleNodeAddTwo() throws Exception {
    // originally 1 peer, add 2 more
    runWithNewCluster(1, cluster -> runTestBootstrapReconf(2, false, cluster));
  }

  @Test
  public void testBootstrapReconf() throws Exception {
    // originally 3 peers, add 2 more
    runWithNewCluster(3, cluster -> runTestBootstrapReconf(2, true, cluster));
  }

  void runTestBootstrapReconf(int numNewPeer, boolean startNewPeer, CLUSTER cluster) throws Exception {
      LOG.info("Originally {} peer(s), add {} more, startNewPeer={}",
          cluster.getNumServers(), numNewPeer, startNewPeer);
      RaftTestUtil.waitForLeader(cluster);
      final RaftPeerId leaderId = cluster.getLeader().getId();
      try (final RaftClient client = cluster.createClient(leaderId)) {

        // submit some msgs before reconf
        for (int i = 0; i < STAGING_CATCHUP_GAP * 2; i++) {
          RaftClientReply reply = client.io().send(new SimpleMessage("m" + i));
          Assert.assertTrue(reply.isSuccess());
        }

        final PeerChanges c1 = cluster.addNewPeers(numNewPeer, startNewPeer);
        LOG.info("Start changing the configuration: {}",
                asList(c1.allPeersInNewConf));
        final AtomicReference<Boolean> success = new AtomicReference<>();

        Thread clientThread = new Thread(() -> {
          try {
            RaftClientReply reply = client.admin().setConfiguration(c1.allPeersInNewConf);
            success.set(reply.isSuccess());
          } catch (IOException ioe) {
            LOG.error("FAILED", ioe);
          }
        });
        clientThread.start();

        if (!startNewPeer) {
          final TimeDuration delay = FIVE_SECONDS;
          LOG.info("delay {} and start new peer(s): {}", delay, c1.newPeers);
          delay.sleep();
          for(RaftPeer p : c1.newPeers) {
            cluster.restartServer(p.getId(), true);
          }
        }
        FIVE_SECONDS.sleep();
        LOG.info(cluster.printServers());
        assertSuccess(success);

        final RaftLog leaderLog = cluster.getLeader().getRaftLog();
        for (RaftPeer newPeer : c1.newPeers) {
          final RaftServer.Division d = cluster.getDivision(newPeer.getId());
          Assert.assertArrayEquals(leaderLog.getEntries(0, Long.MAX_VALUE),
              d.getRaftLog().getEntries(0, Long.MAX_VALUE));
        }
      }
  }

  /**
   * kill the leader before reconfiguration finishes. Make sure the client keeps
   * retrying.
   */
  @Test
  public void testKillLeaderDuringReconf() throws Exception {
    // originally 3 peers
    runWithNewCluster(3, this::runTestKillLeaderDuringReconf);
  }

  void runTestKillLeaderDuringReconf(CLUSTER cluster) throws Exception {
    final AtomicBoolean clientRunning = new AtomicBoolean(true);
    Thread clientThread = null;
    try {
      final RaftPeerId leaderId = RaftTestUtil.waitForLeader(cluster).getId();

      PeerChanges c1 = cluster.addNewPeers(1, false);
      PeerChanges c2 = cluster.removePeers(1, false, asList(c1.newPeers));

      LOG.info("Start setConf: {}", asList(c2.allPeersInNewConf));
      LOG.info(cluster.printServers());

      final CompletableFuture<Void> setConf = new CompletableFuture<>();
      clientThread = new Thread(() -> {
        try(final RaftClient client = cluster.createClient(leaderId)) {
          for(int i = 0; clientRunning.get() && !setConf.isDone(); i++) {
            final RaftClientReply reply = client.admin().setConfiguration(c2.allPeersInNewConf);
            if (reply.isSuccess()) {
              setConf.complete(null);
            }
            LOG.info("setConf attempt #{} failed, {}", i, cluster.printServers());
          }
        } catch(Exception e) {
          LOG.error("Failed to setConf", e);
          setConf.completeExceptionally(e);
        }
      });
      clientThread.start();

      TimeUnit.SECONDS.sleep(1);
      // the leader cannot generate the (old, new) conf, and it will keep
      // bootstrapping the 2 new peers since they have not started yet
      Assert.assertFalse(((RaftConfigurationImpl)cluster.getLeader().getRaftConf()).isTransitional());

      // only (0) the first conf entry, (1) the 1st setConf entry and (2) a metadata entry
      {
        final RaftLog leaderLog = cluster.getLeader().getRaftLog();
        for(LogEntryProto e : RaftTestUtil.getLogEntryProtos(leaderLog)) {
          LOG.info("{}", LogProtoUtils.toLogEntryString(e));
        }
        final long commitIndex = leaderLog.getLastCommittedIndex();
        Assert.assertTrue("commitIndex = " + commitIndex + " > 2", commitIndex <= 2);
      }

      final RaftPeerId killed = RaftTestUtil.waitAndKillLeader(cluster);
      Assert.assertEquals(leaderId, killed);
      final RaftPeerId newLeaderId = RaftTestUtil.waitForLeader(cluster).getId();
      LOG.info("newLeaderId: {}", newLeaderId);

      LOG.info("start new peers: {}", Arrays.asList(c1.newPeers));
      for (RaftPeer np : c1.newPeers) {
        cluster.restartServer(np.getId(), false);
      }

      try {
        setConf.get(10, TimeUnit.SECONDS);
      } catch(TimeoutException ignored) {
      }

      // the client fails with the first leader, and then retry the same setConfiguration request
      waitAndCheckNewConf(cluster, c2.allPeersInNewConf, 2, Collections.singletonList(leaderId));
      setConf.get(1, TimeUnit.SECONDS);
    } finally {
      if (clientThread != null) {
        clientRunning.set(false);
        clientThread.interrupt();
      }
    }
  }

  static void assertSuccess(final AtomicReference<Boolean> success) {
    final String s = "success=" + success;
    Assert.assertNotNull(s, success.get());
    Assert.assertTrue(s, success.get());
  }

  /**
   * When a request's new configuration is the same with the current one, make
   * sure we return success immediately and no log entry is recorded.
   */
  @Test
  public void testNoChangeRequest() throws Exception {
    // originally 3 peers
    runWithNewCluster(3, this::runTestNoChangeRequest);
  }

  void runTestNoChangeRequest(CLUSTER cluster) throws Exception {
    final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster);
    try(final RaftClient client = cluster.createClient(leader.getId())) {
      client.io().send(new SimpleMessage("m"));

      final RaftLog leaderLog = leader.getRaftLog();
      final long committedIndex = leaderLog.getLastCommittedIndex();
      final RaftConfiguration confBefore = cluster.getLeader().getRaftConf();

      // no real configuration change in the request
      final RaftClientReply reply = client.admin().setConfiguration(cluster.getPeers().toArray(RaftPeer.emptyArray()));
      Assert.assertTrue(reply.isSuccess());
      final long newCommittedIndex = leaderLog.getLastCommittedIndex();
      for(long i = committedIndex + 1; i <= newCommittedIndex; i++) {
        final LogEntryProto e = leaderLog.get(i);
        Assert.assertTrue(e.hasMetadataEntry());
      }
      Assert.assertSame(confBefore, cluster.getLeader().getRaftConf());
    }
  }

  /**
   * Make sure a setConfiguration request is rejected if a configuration change
   * is still in progress (i.e., has not been committed yet).
   */
  @Test
  public void testOverlappedSetConfRequests() throws Exception {
    // Make sure leadership check won't affect the test.
    // logSyncDelay.setDelayMs() blocks RaftServerImpl::appendEntriesAsync(),
    // which will disable heartbeats.
    final TimeDuration oldTimeoutMin = RaftServerConfigKeys.Rpc.timeoutMin(getProperties());
    RaftServerConfigKeys.Rpc.setTimeoutMin(getProperties(), TimeDuration.valueOf(2, TimeUnit.SECONDS));
    final TimeDuration oldTimeoutMax = RaftServerConfigKeys.Rpc.timeoutMax(getProperties());
    RaftServerConfigKeys.Rpc.setTimeoutMax(getProperties(), TimeDuration.valueOf(4, TimeUnit.SECONDS));

    // originally 3 peers
    runWithNewCluster(3, this::runTestOverlappedSetConfRequests);

    // reset for the other tests
    RaftServerConfigKeys.Rpc.setTimeoutMin(getProperties(), oldTimeoutMin);
    RaftServerConfigKeys.Rpc.setTimeoutMax(getProperties(), oldTimeoutMax);
  }

  void runTestOverlappedSetConfRequests(CLUSTER cluster) throws Exception {
    try {
      RaftTestUtil.waitForLeader(cluster);

      final RaftPeerId leaderId = cluster.getLeader().getId();

      RaftPeer[] newPeers = cluster.addNewPeers(2, true).allPeersInNewConf;

      // delay every peer's logSync so that the setConf request is delayed
      cluster.getPeers()
          .forEach(peer -> logSyncDelay.setDelayMs(peer.getId().toString(), 1000));

      final CountDownLatch latch = new CountDownLatch(1);
      final RaftPeer[] peersInRequest2 = cluster.getPeers().toArray(new RaftPeer[0]);
      AtomicBoolean caughtException = new AtomicBoolean(false);
      new Thread(() -> {
        try(final RaftClient client2 = cluster.createClient(leaderId)) {
          latch.await();
          LOG.info("client2 starts to change conf");
          final RaftClientRpc sender2 = client2.getClientRpc();
          sender2.sendRequest(cluster.newSetConfigurationRequest(
              client2.getId(), leaderId, peersInRequest2));
        } catch (ReconfigurationInProgressException e) {
          caughtException.set(true);
        } catch (Exception e) {
          LOG.warn("Got unexpected exception when client2 changes conf", e);
        }
      }).start();

      AtomicBoolean confChanged = new AtomicBoolean(false);
      new Thread(() -> {
        try(final RaftClient client1 = cluster.createClient(leaderId)) {
          LOG.info("client1 starts to change conf");
          confChanged.set(client1.admin().setConfiguration(newPeers).isSuccess());
        } catch (IOException e) {
          LOG.warn("Got unexpected exception when client1 changes conf", e);
        }
      }).start();
      Thread.sleep(100);
      latch.countDown();

      for (int i = 0; i < 10 && !confChanged.get(); i++) {
        Thread.sleep(1000);
      }
      Assert.assertTrue(confChanged.get());
      Assert.assertTrue(caughtException.get());
    } finally {
      logSyncDelay.clear();
    }
  }

  /**
   * Test a scenario where the follower truncates its log entries which causes
   * configuration change.
   */
  @Test
  public void testRevertConfigurationChange() throws Exception {
    runWithNewCluster(5, this::runTestRevertConfigurationChange);
  }

  void runTestRevertConfigurationChange(CLUSTER cluster) throws Exception {
    RaftLogBase log2 = null;
    try {
      RaftTestUtil.waitForLeader(cluster);

      final RaftServer.Division leader = cluster.getLeader();
      final RaftPeerId leaderId = leader.getId();

      final RaftLog log = leader.getRaftLog();
      log2 = (RaftLogBase) log;
      Thread.sleep(1000);

      // we block the incoming msg for the leader and block its requests to
      // followers, so that we force the leader change and the old leader will
      // not know
      LOG.info("start blocking the leader");
      BlockRequestHandlingInjection.getInstance().blockReplier(leaderId.toString());
      cluster.setBlockRequestsFrom(leaderId.toString(), true);

      PeerChanges change = cluster.removePeers(1, false, new ArrayList<>());

      AtomicBoolean gotNotLeader = new AtomicBoolean(false);
      final Thread clientThread = new Thread(() -> {
        try(final RaftClient client = cluster.createClient(leaderId)) {
          LOG.info("client starts to change conf");
          final RaftClientRpc sender = client.getClientRpc();
          RaftClientReply reply = sender.sendRequest(cluster.newSetConfigurationRequest(
              client.getId(), leaderId, change.allPeersInNewConf));
          if (reply.getNotLeaderException() != null) {
            gotNotLeader.set(true);
          }
        } catch (IOException e) {
          LOG.warn("Got unexpected exception when client1 changes conf", e);
        }
      });
      clientThread.start();

      // find ConfigurationEntry
      final TimeDuration sleepTime = TimeDuration.valueOf(500, TimeUnit.MILLISECONDS);
      final long confIndex = JavaUtils.attemptRepeatedly(() -> {
        final long last = log.getLastEntryTermIndex().getIndex();
        for (long i = last; i >= 1; i--) {
          if (log.get(i).hasConfigurationEntry()) {
            return i;
          }
        }
        throw new Exception("ConfigurationEntry not found: last=" + last);
      }, 10, sleepTime, "confIndex", LOG);

      // wait till the old leader persist the new conf
      JavaUtils.attemptRepeatedly(() -> {
        Assert.assertTrue(log.getFlushIndex() >= confIndex);
        return null;
      }, 10, sleepTime, "FLUSH", LOG);
      final long committed = log.getLastCommittedIndex();
      Assert.assertTrue(committed < confIndex);

      // unblock the old leader
      BlockRequestHandlingInjection.getInstance().unblockReplier(leaderId.toString());
      cluster.setBlockRequestsFrom(leaderId.toString(), false);

      // the client should get NotLeaderException
      clientThread.join(5000);
      Assert.assertTrue(gotNotLeader.get());

      // the old leader should have truncated the setConf from the log
      JavaUtils.attemptRepeatedly(() -> {
        Assert.assertTrue(log.getLastCommittedIndex() >= confIndex);
        return null;
      }, 10, ONE_SECOND, "COMMIT", LOG);
      Assert.assertTrue(log.get(confIndex).hasConfigurationEntry());
      log2 = null;
    } finally {
      RaftStorageTestUtils.printLog(log2, s -> LOG.info(s));
    }
  }

  /**
   * Delay the commit of the leader placeholder log entry and see if the client
   * can correctly receive and handle the LeaderNotReadyException.
   */
  @Test
  public void testLeaderNotReadyException() throws Exception {
    LOG.info("Start testLeaderNotReadyException");
    final MiniRaftCluster cluster = newCluster(1).initServers();
    try {
      // delay 1s for each logSync call
      cluster.getServers().forEach(
          peer -> leaderPlaceHolderDelay.setDelayMs(peer.getId().toString(), 2000));
      cluster.start();

      AtomicBoolean caughtNotReady = new AtomicBoolean(false);
      AtomicBoolean success = new AtomicBoolean(false);
      final RaftPeerId leaderId = cluster.getPeers().iterator().next().getId();
      new Thread(() -> {
        try (final RaftClient client = cluster.createClient(leaderId)) {
          final RaftClientRpc sender = client.getClientRpc();
          final RaftClientRequest request = cluster.newRaftClientRequest(
                  client.getId(), leaderId, new SimpleMessage("test"));
          while (!success.get()) {
            try {
              final RaftClientReply reply = sender.sendRequest(request);
              success.set(reply.isSuccess());
              if (reply.getException() != null && reply.getException() instanceof LeaderNotReadyException) {
                caughtNotReady.set(true);
              }
            } catch (IOException e) {
              LOG.info("Hit other IOException", e);
            }
            if (!success.get()) {
              try {
                Thread.sleep(200);
              } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
              }
            }
          }
        } catch (Exception ignored) {
          LOG.warn("{} is ignored", JavaUtils.getClassSimpleName(ignored.getClass()), ignored);
        }
      }).start();

      RaftTestUtil.waitForLeader(cluster);
      for (int i = 0; !success.get() && i < 5; i++) {
        Thread.sleep(1000);
      }
      Assert.assertTrue(success.get());
      Assert.assertTrue(caughtNotReady.get());
    } finally {
      leaderPlaceHolderDelay.clear();
      cluster.shutdown();
    }
  }
}
