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
import org.apache.ratis.MiniRaftCluster;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.LogUtils;
import org.apache.ratis.util.function.CheckedBiConsumer;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public abstract class GroupManagementBaseTest extends BaseTest {
  static final Logger LOG = LoggerFactory.getLogger(GroupManagementBaseTest.class);

  {
    LogUtils.setLogLevel(RaftServerProxy.LOG, Level.DEBUG);
    LogUtils.setLogLevel(RaftServerImpl.LOG, Level.DEBUG);
    LogUtils.setLogLevel(RaftClient.LOG, Level.DEBUG);
  }

  static final RaftProperties prop = new RaftProperties();

  public abstract MiniRaftCluster.Factory<? extends MiniRaftCluster> getClusterFactory();

  public MiniRaftCluster getCluster(int peerNum) throws IOException {
    return getClusterFactory().newCluster(peerNum, prop);
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
    final RaftClient client = cluster.createClient(newGroup);
    for(RaftPeer p : newGroup.getPeers()) {
      client.groupAdd(newGroup, p.getId());
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
    final String type = cluster.getClass().getSimpleName()
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
          client.groupAdd(groups[i], p.getId());
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
          final File root = cluster.getServer(p.getId()).getImpl(g.getGroupId()).getState().getStorage().getStorageDir().getRoot();
          Assert.assertTrue(root.exists());
          Assert.assertTrue(root.isDirectory());

          final RaftClientReply r;
          try (final RaftClient client = cluster.createClient(p.getId(), g)) {
            r = client.groupRemove(g.getGroupId(), true, p.getId());
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
            client.groupAdd(newGroup, p.getId());
          }
        }
      }
    }
    LOG.info(chosen + ") setConfiguration: " + cluster.printServers(groups[chosen].getGroupId()));
    try (final RaftClient client = cluster.createClient(groups[chosen])) {
      client.setConfiguration(allPeers.toArray(RaftPeer.emptyArray()));
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
}
