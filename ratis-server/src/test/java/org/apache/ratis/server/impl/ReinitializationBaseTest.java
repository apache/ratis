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
import org.apache.ratis.MiniRaftCluster;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.ConfUtils;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.LogUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public abstract class ReinitializationBaseTest {
  static {
    LogUtils.setLogLevel(RaftServerImpl.LOG, Level.DEBUG);
    LogUtils.setLogLevel(RaftClient.LOG, Level.DEBUG);
    LogUtils.setLogLevel(ConfUtils.LOG, Level.OFF);
  }
  static final Logger LOG = LoggerFactory.getLogger(ReinitializationBaseTest.class);

  static final RaftProperties prop = new RaftProperties();

  public abstract MiniRaftCluster.Factory<? extends MiniRaftCluster> getClusterFactory();

  public MiniRaftCluster getCluster(int peerNum) throws IOException {
    return getClusterFactory().newCluster(peerNum, prop);
  }

  @Test
  public void testReinitialize() throws Exception {
    final MiniRaftCluster cluster = getCluster(0);
    LOG.info("Start testReinitialize" + cluster.printServers());

    // Start server with an empty conf
    final RaftGroupId groupId = RaftGroupId.createId();
    final RaftGroup group = new RaftGroup(groupId, RaftPeer.EMPTY_PEERS);

    final List<RaftPeerId> ids = Arrays.stream(MiniRaftCluster.generateIds(3, 0))
        .map(RaftPeerId::valueOf).collect(Collectors.toList());
    ids.forEach(id -> cluster.putNewServer(id, group, true));
    LOG.info("putNewServer: " + cluster.printServers());

    cluster.start();
    LOG.info("start: " + cluster.printServers());

    // Make sure that there are no leaders.
    TimeUnit.SECONDS.sleep(1);
    Assert.assertNull(cluster.getLeader());

    // Reinitialize servers
    final RaftPeer[] peers = cluster.getPeers().toArray(RaftPeer.EMPTY_PEERS);
    for(RaftPeer p : peers) {
      final RaftClient client = cluster.createClient(p.getId(),
          new RaftGroup(groupId, new RaftPeer[]{p}));
      client.reinitialize(peers, p.getId());
    }
    Assert.assertNotNull(RaftTestUtil.waitForLeader(cluster, true));
    cluster.shutdown();
  }

  @Test
  public void testReinitialize5Nodes() throws Exception {
    final int[] idIndex = {3, 4, 5};
    runTestReinitializeMultiGroups(idIndex, 0);
  }

  @Test
  public void testReinitialize7Nodes() throws Exception {
    final int[] idIndex = {1, 6, 7};
    runTestReinitializeMultiGroups(idIndex, 1);
  }

  @Test
  public void testReinitialize9Nodes() throws Exception {
    final int[] idIndex = {5, 8, 9};
    runTestReinitializeMultiGroups(idIndex, 0);
  }

  private void runTestReinitializeMultiGroups(int[] idIndex, int chosen) throws Exception {
    printThreadCount(null, "init");
    final MiniRaftCluster cluster = getCluster(0);

    if (chosen < 0) {
      chosen = ThreadLocalRandom.current().nextInt(idIndex.length);
    }
    final String type = cluster.getClass().getSimpleName()
        + Arrays.toString(idIndex) + "chosen=" + chosen;
    LOG.info("\n\nrunTestReinitializeMultiGroups with " + type + ": " + cluster.printServers());

    // Start server with an empty conf
    final RaftGroup emptyGroup = new RaftGroup(RaftGroupId.createId(), RaftPeer.EMPTY_PEERS);

    final List<RaftPeerId> ids = Arrays.stream(MiniRaftCluster.generateIds(idIndex[idIndex.length - 1], 0))
        .map(RaftPeerId::valueOf).collect(Collectors.toList());
    ids.forEach(id -> cluster.putNewServer(id, emptyGroup, true));
    LOG.info("putNewServer: " + cluster.printServers());

    cluster.start();
    LOG.info("start: " + cluster.printServers());

    // Make sure that there are no leaders.
    TimeUnit.SECONDS.sleep(1);
    Assert.assertNull(cluster.getLeader());

    // Reinitialize servers to three groups
    final List<RaftPeer> allPeers = cluster.getPeers();
    Collections.sort(allPeers, Comparator.comparing(p -> p.getId().toString()));
    final RaftGroup[] groups = new RaftGroup[idIndex.length];
    for (int i = 0; i < idIndex.length; i++) {
      final RaftGroupId gid = RaftGroupId.createId();
      final int previous = i == 0 ? 0 : idIndex[i - 1];
      final RaftPeer[] peers = allPeers.subList(previous, idIndex[i]).toArray(RaftPeer.EMPTY_PEERS);
      groups[i] = new RaftGroup(gid, peers);

      LOG.info(i + ") starting " + groups[i]);
      for(RaftPeer p : peers) {
        try(final RaftClient client = cluster.createClient(p.getId(), groups[i])) {
          client.reinitialize(peers, p.getId());
        }
      }
      Assert.assertNotNull(RaftTestUtil.waitForLeader(cluster, true, gid));
    }
    printThreadCount(type, "start groups");
    LOG.info("start groups: " + cluster.printServers());

    // randomly close two of the groups (i.e. reinitialize to empty peers)
    LOG.info("chosen = " + chosen + ", " + groups[chosen]);

    for (int i = 0; i < groups.length; i++) {
      if (i != chosen) {
        final RaftGroup g = groups[i];
        LOG.info(i + ") close " + cluster.printServers(g.getGroupId()));
        for(RaftPeer p : g.getPeers()) {
          try (final RaftClient client = cluster.createClient(p.getId(), g)) {
            client.reinitialize(RaftPeer.EMPTY_PEERS, p.getId());
          }
        }
      }
    }
    printThreadCount(type, "close groups");
    LOG.info("close groups: " + cluster.printServers());

    // update chosen group to use all the peers
    final RaftPeer[] array = allPeers.toArray(RaftPeer.EMPTY_PEERS);
    for(int i = 0; i < groups.length; i++) {
      LOG.info(i + ") update " + cluster.printServers(groups[i].getGroupId()));
      if (i == chosen) {
        try (final RaftClient client = cluster.createClient(null, groups[i])) {
          client.setConfiguration(array);
        }
      } else {
        for(RaftPeer p : groups[i].getPeers()) {
          try (final RaftClient client = cluster.createClient(p.getId(), groups[i])) {
            client.reinitialize(array, p.getId());
          }
        }
      }
    }
    Assert.assertNotNull(RaftTestUtil.waitForLeader(cluster, true));
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
