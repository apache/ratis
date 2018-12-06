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
package org.apache.ratis;

import org.apache.log4j.Level;
import org.apache.ratis.RaftTestUtil.SimpleMessage;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientRpc;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.storage.RaftLog;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.LogUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

public abstract class RaftExceptionBaseTest<CLUSTER extends MiniRaftCluster>
    extends BaseTest
    implements MiniRaftCluster.Factory.Get<CLUSTER> {
  static {
    LogUtils.setLogLevel(RaftServerImpl.LOG, Level.DEBUG);
    LogUtils.setLogLevel(RaftLog.LOG, Level.DEBUG);
    LogUtils.setLogLevel(RaftClient.LOG, Level.DEBUG);
  }

  public static final int NUM_PEERS = 3;

  private CLUSTER cluster;

  @Before
  public void setup() throws IOException {
    final RaftProperties prop = getProperties();
    RaftServerConfigKeys.Log.Appender
        .setBufferByteLimit(prop, SizeInBytes.valueOf("4KB"));
    cluster = newCluster(NUM_PEERS);
    cluster.start();
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testHandleNotLeaderException() throws Exception {
    testHandleNotLeaderException(false);
  }

  /**
   * Test handle both IOException and NotLeaderException
   */
  @Test
  public void testHandleNotLeaderAndIOException() throws Exception {
    testHandleNotLeaderException(true);
  }

  private void testHandleNotLeaderException(boolean killNewLeader)
      throws Exception {
    RaftTestUtil.waitForLeader(cluster);
    final RaftPeerId leaderId = cluster.getLeader().getId();
    final RaftClient client = cluster.createClient(leaderId);

    RaftClientReply reply = client.send(new SimpleMessage("m1"));
    Assert.assertTrue(reply.isSuccess());

    // enforce leader change
    RaftPeerId newLeader = RaftTestUtil.changeLeader(cluster, leaderId);

    if (killNewLeader) {
      // kill the new leader
      cluster.killServer(newLeader);
    }

    RaftClientRpc rpc = client.getClientRpc();
    reply= null;
    for (int i = 0; reply == null && i < 10; i++) {
      try {
        reply = rpc.sendRequest(cluster.newRaftClientRequest(
            ClientId.randomId(), leaderId, new SimpleMessage("m2")));
      } catch (IOException ignored) {
        Thread.sleep(1000);
      }
    }
    Assert.assertNotNull(reply);
    Assert.assertFalse(reply.isSuccess());
    final NotLeaderException nle = reply.getNotLeaderException();
    Objects.requireNonNull(nle);
    Assert.assertEquals(newLeader, nle.getSuggestedLeader().getId());

    reply = client.send(new SimpleMessage("m3"));
    Assert.assertTrue(reply.isSuccess());
    client.close();
  }

  @Test
  public void testNotLeaderExceptionWithReconf() throws Exception {
    Assert.assertNotNull(RaftTestUtil.waitForLeader(cluster));

    final RaftPeerId leaderId = cluster.getLeader().getId();
    final RaftClient client = cluster.createClient(leaderId);

    // enforce leader change
    RaftPeerId newLeader = RaftTestUtil.changeLeader(cluster, leaderId);

    // also add two new peers
    // add two more peers
    MiniRaftCluster.PeerChanges change = cluster.addNewPeers(
        new String[]{"ss1", "ss2"}, true);
    // trigger setConfiguration
    LOG.info("Start changing the configuration: {}",
        Arrays.asList(change.allPeersInNewConf));
    try(final RaftClient c2 = cluster.createClient(newLeader)) {
      RaftClientReply reply = c2.setConfiguration(change.allPeersInNewConf);
      Assert.assertTrue(reply.isSuccess());
    }
    LOG.info(cluster.printServers());

    RaftClientRpc rpc = client.getClientRpc();
    RaftClientReply reply = null;
    // it is possible that the remote peer's rpc server is not ready. need retry
    for (int i = 0; reply == null && i < 10; i++) {
      try {
        reply = rpc.sendRequest(cluster.newRaftClientRequest(
            ClientId.randomId(), leaderId, new SimpleMessage("m1")));
      } catch (IOException ignored) {
        Thread.sleep(1000);
      }
    }
    Assert.assertNotNull(reply);
    Assert.assertFalse(reply.isSuccess());
    final NotLeaderException nle = reply.getNotLeaderException();
    Objects.requireNonNull(nle);
    Assert.assertEquals(newLeader, nle.getSuggestedLeader().getId());
    Collection<RaftPeer> peers = cluster.getPeers();
    RaftPeer[] peersFromReply = reply.getNotLeaderException().getPeers();
    Assert.assertEquals(peers.size(), peersFromReply.length);
    for (RaftPeer p : peersFromReply) {
      Assert.assertTrue(peers.contains(p));
    }

    reply = client.send(new SimpleMessage("m2"));
    Assert.assertTrue(reply.isSuccess());
    client.close();
  }

  @Test
  public void testGroupMismatchException() throws Exception {
    final RaftGroup clusterGroup = cluster.getGroup();
    Assert.assertEquals(NUM_PEERS, clusterGroup.getPeers().size());

    final RaftGroup anotherGroup = RaftGroup.valueOf(RaftGroupId.randomId(), clusterGroup.getPeers());
    Assert.assertNotEquals(clusterGroup.getGroupId(), anotherGroup.getGroupId());

    // Create client using another group
    try(RaftClient client = cluster.createClient(anotherGroup)) {
      testFailureCase("send(..) with client group being different from the server group",
          () -> client.send(Message.EMPTY),
          GroupMismatchException.class);

      testFailureCase("sendReadOnly(..) with client group being different from the server group",
          () -> client.sendReadOnly(Message.EMPTY),
          GroupMismatchException.class);

      testFailureCase("setConfiguration(..) with client group being different from the server group",
          () -> client.setConfiguration(RaftPeer.emptyArray()),
          GroupMismatchException.class);

      testFailureCase("groupRemove(..) with another group id",
          () -> client.groupRemove(anotherGroup.getGroupId(), false, clusterGroup.getPeers().iterator().next().getId()),
          GroupMismatchException.class);
    }
  }

  @Test
  public void testStaleReadException() throws Exception {
    RaftTestUtil.waitForLeader(cluster);
    try (RaftClient client = cluster.createClient()) {
      final RaftPeerId follower = cluster.getFollowers().iterator().next().getId();
      testFailureCase("sendStaleRead(..) with a large commit index",
          () -> client.sendStaleRead(Message.EMPTY, 1_000_000_000L, follower),
          StateMachineException.class, StaleReadException.class);
    }
  }

  @Test
  public void testLogAppenderBufferCapacity() throws Exception {
    RaftTestUtil.waitForLeader(cluster);
    final RaftPeerId leaderId = cluster.getLeader().getId();
    final RaftClient client = cluster.createClient(leaderId);
    byte[] bytes = new byte[8192];
    Arrays.fill(bytes, (byte) 1);
    SimpleMessage msg =
        new SimpleMessage(new String(bytes));
    try {
      client.send(msg);
      Assert.fail("Expected StateMachineException  not thrown");
    } catch (StateMachineException sme) {
      Assert.assertTrue(sme.getMessage()
          .contains("exceeds the max buffer limit"));
    }
  }
}
