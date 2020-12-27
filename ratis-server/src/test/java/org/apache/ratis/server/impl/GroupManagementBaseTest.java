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
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.GroupManagementApi;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.exceptions.AlreadyExistsException;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Log4jUtils;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.function.CheckedBiConsumer;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.Random;
import java.util.stream.Collectors;

public abstract class GroupManagementBaseTest extends BaseTest {
  static final Logger LOG = LoggerFactory.getLogger(GroupManagementBaseTest.class);

  {
    Log4jUtils.setLogLevel(RaftServer.LOG, Level.DEBUG);
    Log4jUtils.setLogLevel(RaftServer.Division.LOG, Level.DEBUG);
    Log4jUtils.setLogLevel(RaftClient.LOG, Level.DEBUG);
  }

  static final RaftProperties prop = new RaftProperties();

  static {
    // avoid flaky behaviour in CI environment
    RaftServerConfigKeys.Rpc.setTimeoutMin(prop, TimeDuration.valueOf(300, TimeUnit.MILLISECONDS));
    RaftServerConfigKeys.Rpc.setTimeoutMax(prop, TimeDuration.valueOf(600, TimeUnit.MILLISECONDS));
  }

  public abstract MiniRaftCluster.Factory<? extends MiniRaftCluster> getClusterFactory();

  public MiniRaftCluster getCluster(int peerNum) {
    return getClusterFactory().newCluster(peerNum, prop);
  }

  @Test
  public void testGroupWithPriority() throws Exception {
    final MiniRaftCluster cluster = getCluster(0);
    LOG.info("Start testMultiGroup" + cluster.printServers());

    // Start server with null group
    final List<RaftPeerId> ids = Arrays.stream(MiniRaftCluster.generateIds(3, 0))
        .map(RaftPeerId::valueOf).collect(Collectors.toList());
    ids.forEach(id -> cluster.putNewServer(id, null, true));
    LOG.info("putNewServer: " + cluster.printServers());

    cluster.start();

    // Make sure that there are no leaders.
    TimeUnit.SECONDS.sleep(1);
    LOG.info("start: " + cluster.printServers());
    Assert.assertNull(cluster.getLeader());

    // Add groups
    List<RaftPeer> peers = cluster.getPeers();
    Random r = new Random(1);
    final int suggestedLeaderIndex = r.nextInt(peers.size());
    List<RaftPeer> peersWithPriority = getPeersWithPriority(peers, peers.get(suggestedLeaderIndex));
    final RaftGroup newGroup = RaftGroup.valueOf(RaftGroupId.randomId(), peersWithPriority);
    LOG.info("add new group: " + newGroup);
    try (final RaftClient client = cluster.createClient(newGroup)) {
      // Before request, client try leader with the highest priority
      Assert.assertTrue(client.getLeaderId() == peersWithPriority.get(suggestedLeaderIndex).getId());
      for (RaftPeer p : newGroup.getPeers()) {
        client.getGroupManagementApi(p.getId()).add(newGroup);
      }
    }

    JavaUtils.attempt(() -> {
      final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster, newGroup.getGroupId());
      Assert.assertTrue(leader.getId() == peers.get(suggestedLeaderIndex).getId());
    }, 10, TimeDuration.valueOf(1, TimeUnit.SECONDS), "testMultiGroupWithPriority", LOG);

    String suggestedLeader = peers.get(suggestedLeaderIndex).getId().toString();

    // isolate leader, then follower will trigger leader election.
    // Because leader was isolated, so leader can not vote, and candidate wait timeout,
    // then if candidate get majority, candidate can pass vote
    BlockRequestHandlingInjection.getInstance().blockRequestor(suggestedLeader);
    BlockRequestHandlingInjection.getInstance().blockReplier(suggestedLeader);
    cluster.setBlockRequestsFrom(suggestedLeader, true);

    JavaUtils.attempt(() -> {
      final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster, newGroup.getGroupId());
      Assert.assertTrue(leader.getId() != peers.get(suggestedLeaderIndex).getId());
    }, 10, TimeDuration.valueOf(1, TimeUnit.SECONDS), "testMultiGroupWithPriority", LOG);

    // send request so that suggested leader's log lag behind new leader's,
    // when suggested leader rejoin cluster, it will catch up log first.
    try (final RaftClient client = cluster.createClient(newGroup)) {
      for (int i = 0; i < 10; i ++) {
        RaftClientReply reply = client.io().send(new RaftTestUtil.SimpleMessage("m" + i));
        Assert.assertTrue(reply.isSuccess());
      }
    }

    BlockRequestHandlingInjection.getInstance().unblockRequestor(suggestedLeader);
    BlockRequestHandlingInjection.getInstance().unblockReplier(suggestedLeader);
    cluster.setBlockRequestsFrom(suggestedLeader, false);

    // suggested leader with highest priority rejoin cluster, then current leader will yield
    // leadership to suggested leader when suggested leader catch up the log.
    JavaUtils.attempt(() -> {
      final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster, newGroup.getGroupId());
      Assert.assertTrue(leader.getId() == peers.get(suggestedLeaderIndex).getId());
    }, 10, TimeDuration.valueOf(1, TimeUnit.SECONDS), "testMultiGroupWithPriority", LOG);

    // change the suggest leader
    final int newSuggestedLeaderIndex = (suggestedLeaderIndex + 1) % peersWithPriority.size();
    List<RaftPeer> peersWithNewPriority = getPeersWithPriority(peers, peers.get(newSuggestedLeaderIndex));
    try (final RaftClient client = cluster.createClient(newGroup)) {
      RaftClientReply reply = client.admin().setConfiguration(peersWithNewPriority.toArray(new RaftPeer[0]));
      Assert.assertTrue(reply.isSuccess());
    }

    JavaUtils.attempt(() -> {
      final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster, newGroup.getGroupId());
      Assert.assertTrue(leader.getId() == peers.get(newSuggestedLeaderIndex).getId());
    }, 10, TimeDuration.valueOf(1, TimeUnit.SECONDS), "testMultiGroupWithPriority", LOG);

    cluster.killServer(peers.get(newSuggestedLeaderIndex).getId());
    JavaUtils.attempt(() -> {
      final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster, newGroup.getGroupId());
      Assert.assertTrue(leader.getId() != peers.get(newSuggestedLeaderIndex).getId());
    }, 10, TimeDuration.valueOf(1, TimeUnit.SECONDS), "testMultiGroupWithPriority", LOG);

    cluster.shutdown();
  }

  @Test
  public void testSingleGroupRestart() throws Exception {
    final MiniRaftCluster cluster = getCluster(0);
    LOG.info("Start testMultiGroup" + cluster.printServers());

    // Start server with null group
    final List<RaftPeerId> ids = Arrays.stream(MiniRaftCluster.generateIds(3, 0))
        .map(RaftPeerId::valueOf).collect(Collectors.toList());
    ids.forEach(id -> cluster.putNewServer(id, null, true));
    LOG.info("putNewServer: " + cluster.printServers());

    cluster.start();

    // Make sure that there are no leaders.
    TimeUnit.SECONDS.sleep(1);
    LOG.info("start: " + cluster.printServers());
    Assert.assertNull(cluster.getLeader());

    // Add groups
    final RaftGroup newGroup = RaftGroup.valueOf(RaftGroupId.randomId(), cluster.getPeers());
    LOG.info("add new group: " + newGroup);
    try (final RaftClient client = cluster.createClient(newGroup)) {
      for (RaftPeer p : newGroup.getPeers()) {
        client.getGroupManagementApi(p.getId()).add(newGroup);
      }
    }
    Assert.assertNotNull(RaftTestUtil.waitForLeader(cluster));
    TimeUnit.SECONDS.sleep(1);

    // restart the servers with null group
    LOG.info("restart servers");
    for(RaftPeer p : newGroup.getPeers()) {
      cluster.restartServer(p.getId(), null, false);
    }

    // the servers should retrieve the conf from the log.
    Assert.assertNotNull(RaftTestUtil.waitForLeader(cluster));

    cluster.shutdown();
  }

  @Test
  public void testMultiGroup5Nodes() throws Exception {
    final int[] idIndex = {3, 4, 5};
    runMultiGroupTest(idIndex, 0);
  }

  @Test
  public void testMultiGroup7Nodes() throws Exception {
    final int[] idIndex = {1, 6, 7};
    runMultiGroupTest(idIndex, 1);
  }

  @Test
  public void testMultiGroup9Nodes() throws Exception {
    final int[] idIndex = {5, 8, 9};
    runMultiGroupTest(idIndex, 2);
  }

  private void runMultiGroupTest(int[] idIndex, int chosen) throws Exception {
    printThreadCount(null, "init");
    runMultiGroupTest(getCluster(0), idIndex, chosen, NOOP);
  }

  static final CheckedBiConsumer<MiniRaftCluster, RaftGroup, RuntimeException> NOOP = (c, g) -> {};

  public static <T extends Throwable> void runMultiGroupTest(
      MiniRaftCluster cluster, int[] idIndex, int chosen,
      CheckedBiConsumer<MiniRaftCluster, RaftGroup, T> checker)
      throws IOException, InterruptedException, T {
    if (chosen < 0) {
      chosen = ThreadLocalRandom.current().nextInt(idIndex.length);
    }
    final String type = JavaUtils.getClassSimpleName(cluster.getClass())
        + Arrays.toString(idIndex) + "chosen=" + chosen;
    LOG.info("\n\nrunMultiGroupTest with " + type + ": " + cluster.printServers());

    // Start server with an empty conf
    final RaftGroup emptyGroup = RaftGroup.valueOf(cluster.getGroupId());

    final List<RaftPeerId> ids = Arrays.stream(MiniRaftCluster.generateIds(idIndex[idIndex.length - 1], 0))
        .map(RaftPeerId::valueOf).collect(Collectors.toList());
    LOG.info("ids: " + ids);
    ids.forEach(id -> cluster.putNewServer(id, emptyGroup, true));
    LOG.info("putNewServer: " + cluster.printServers());

    TimeUnit.SECONDS.sleep(1);
    cluster.start();

    // Make sure that there are no leaders.
    TimeUnit.SECONDS.sleep(1);
    LOG.info("start: " + cluster.printServers());
    Assert.assertNull(cluster.getLeader());

    // Reinitialize servers to three groups
    final List<RaftPeer> allPeers = cluster.getPeers();
    Collections.sort(allPeers, Comparator.comparing(p -> p.getId().toString()));
    final RaftGroup[] groups = new RaftGroup[idIndex.length];
    for (int i = 0; i < idIndex.length; i++) {
      final RaftGroupId gid = RaftGroupId.randomId();
      final int previous = i == 0 ? 0 : idIndex[i - 1];
      final RaftPeer[] peers = allPeers.subList(previous, idIndex[i]).toArray(RaftPeer.emptyArray());
      groups[i] = RaftGroup.valueOf(gid, peers);

      LOG.info(i + ") starting " + groups[i]);
      for(RaftPeer p : peers) {
        try(final RaftClient client = cluster.createClient(p.getId(), emptyGroup)) {
          client.getGroupManagementApi(p.getId()).add(groups[i]);
        }
      }
      Assert.assertNotNull(RaftTestUtil.waitForLeader(cluster, gid));
      checker.accept(cluster, groups[i]);
    }
    printThreadCount(type, "start groups");
    LOG.info("start groups: " + cluster.printServers());

    // randomly remove two of the groups
    LOG.info("chosen = " + chosen + ", " + groups[chosen]);

    for (int i = 0; i < groups.length; i++) {
      if (i != chosen) {
        final RaftGroup g = groups[i];
        LOG.info(i + ") close " + cluster.printServers(g.getGroupId()));
        for(RaftPeer p : g.getPeers()) {
          final RaftServer.Division d = cluster.getDivision(p.getId(), g.getGroupId());
          final File root = d.getRaftStorage().getStorageDir().getRoot();
          Assert.assertTrue(root.exists());
          Assert.assertTrue(root.isDirectory());

          final RaftClientReply r;
          try (final RaftClient client = cluster.createClient(p.getId(), g)) {
            r = client.getGroupManagementApi(p.getId()).remove(g.getGroupId(), true, false);
          }
          Assert.assertTrue(r.isSuccess());
          Assert.assertFalse(root.exists());
        }
      }
    }
    printThreadCount(type, "close groups");
    LOG.info("close groups: " + cluster.printServers());

    // update chosen group to use all the peers
    final RaftGroup newGroup = RaftGroup.valueOf(groups[chosen].getGroupId());
    for(int i = 0; i < groups.length; i++) {
      if (i != chosen) {
        LOG.info(i + ") groupAdd: " + cluster.printServers(groups[i].getGroupId()));
        for (RaftPeer p : groups[i].getPeers()) {
          try (final RaftClient client = cluster.createClient(p.getId(), groups[i])) {
            client.getGroupManagementApi(p.getId()).add(newGroup);
          }
        }
      }
    }
    LOG.info(chosen + ") setConfiguration: " + cluster.printServers(groups[chosen].getGroupId()));
    try (final RaftClient client = cluster.createClient(groups[chosen])) {
      client.admin().setConfiguration(allPeers.toArray(RaftPeer.emptyArray()));
    }

    Assert.assertNotNull(RaftTestUtil.waitForLeader(cluster));
    checker.accept(cluster, groups[chosen]);
    LOG.info("update groups: " + cluster.printServers());
    printThreadCount(type, "update groups");

    cluster.shutdown();
    printThreadCount(type, "shutdown");
  }

  static void printThreadCount(String type, String label) {
    System.out.println("| " + type + " | " + label + " | "
        + JavaUtils.getRootThreadGroup().activeCount() + " |");
  }

  @Test
  public void testGroupAlreadyExists() throws Exception {
    final MiniRaftCluster cluster = getCluster(1);
    cluster.start();
    final RaftPeer peer = cluster.getPeers().get(0);
    final RaftPeerId peerId = peer.getId();
    final RaftGroup group = RaftGroup.valueOf(cluster.getGroupId(), peer);
    try (final RaftClient client = cluster.createClient()) {
      Assert.assertEquals(group, cluster.getDivision(peerId).getGroup());
      try {
        client.getGroupManagementApi(peer.getId()).add(group);
      } catch (IOException ex) {
        // HadoopRPC throws RemoteException, which makes it hard to check if
        // the exception is instance of AlreadyExistsException
        Assert.assertTrue(ex.toString().contains(AlreadyExistsException.class.getCanonicalName()));
      }
      Assert.assertEquals(group, cluster.getDivision(peerId).getGroup());
      cluster.shutdown();
    }
  }

  @Test
  public void testGroupRemoveWhenRename() throws Exception {
    final MiniRaftCluster cluster1 = getCluster(1);
    RaftServerConfigKeys.setRemovedGroupsDir(cluster1.getProperties(),
        Files.createTempDirectory("groups").toFile());
    final MiniRaftCluster cluster2 = getCluster(3);
    cluster1.start();
    final RaftPeer peer1 = cluster1.getPeers().get(0);
    final RaftPeerId peerId1 = peer1.getId();
    final RaftGroup group1 = RaftGroup.valueOf(cluster1.getGroupId(), peer1);
    final RaftGroup group2 = RaftGroup.valueOf(cluster2.getGroupId(), peer1);
    try (final RaftClient client = cluster1.createClient()) {
      Assert.assertEquals(group1, cluster1.getDivision(peerId1).getGroup());
      try {

        // Group2 is added to one of the peers in Group1
        final GroupManagementApi api1 = client.getGroupManagementApi(peerId1);
        api1.add(group2);
        List<RaftGroupId> groupIds1 = cluster1.getServer(peerId1).getGroupIds();
        Assert.assertEquals(groupIds1.size(), 2);

        // Group2 is renamed from the peer1 of Group1
        api1.remove(group2.getGroupId(), false, true);

        groupIds1 = cluster1.getServer(peerId1).getGroupIds();
        Assert.assertEquals(groupIds1.size(), 1);
        cluster1.restart(false);

        List<RaftGroupId> groupIdsAfterRestart =
            cluster1.getServer(peerId1).getGroupIds();
        Assert.assertEquals(groupIds1.size(), groupIdsAfterRestart.size());

        File renamedGroup = new File(RaftServerConfigKeys.removedGroupsDir(
            cluster1.getProperties()), group2.getGroupId().getUuid().toString());
        Assert.assertTrue(renamedGroup.isDirectory());

      } catch (IOException ex) {
        Assert.fail();
      } finally {
        cluster1.shutdown();
        // Clean up
        FileUtils.deleteFully(RaftServerConfigKeys.removedGroupsDir(
            cluster1.getProperties()));
      }
    }
  }

  @Test
  public void testGroupRemoveWhenDelete() throws Exception {
    final MiniRaftCluster cluster1 = getCluster(1);
        RaftServerConfigKeys.setRemovedGroupsDir(cluster1.getProperties(),
            Files.createTempDirectory("groups").toFile());
    final MiniRaftCluster cluster2 = getCluster(3);
    cluster1.start();
    final RaftPeer peer1 = cluster1.getPeers().get(0);
    final RaftPeerId peerId1 = peer1.getId();
    final RaftGroup group1 = RaftGroup.valueOf(cluster1.getGroupId(), peer1);
    final RaftGroup group2 = RaftGroup.valueOf(cluster2.getGroupId(), peer1);
    try (final RaftClient client = cluster1.createClient()) {
      Assert.assertEquals(group1,
          cluster1.getDivision(peerId1).getGroup());
      try {

        // Group2 is added again to one of the peers in Group1
        final GroupManagementApi api1 = client.getGroupManagementApi(peerId1);
        api1.add(group2);
        List<RaftGroupId> groupIds1 = cluster1.getServer(peerId1).getGroupIds();
        Assert.assertEquals(groupIds1.size(), 2);

        // Group2 is deleted from the peer1 of Group1
        api1.remove(group2.getGroupId(), true, false);

        groupIds1 = cluster1.getServer(peerId1).getGroupIds();
        Assert.assertEquals(groupIds1.size(), 1);
        cluster1.restart(false);
        List<RaftGroupId> groupIdsAfterRestart =
            cluster1.getServer(peerId1).getGroupIds();
        Assert.assertEquals(groupIds1.size(), groupIdsAfterRestart.size());

      } catch (IOException ex) {
        Assert.fail();
      } finally {
        cluster1.shutdown();
        FileUtils.deleteFully(RaftServerConfigKeys.removedGroupsDir(
            cluster1.getProperties()));
      }
    }
  }

}
