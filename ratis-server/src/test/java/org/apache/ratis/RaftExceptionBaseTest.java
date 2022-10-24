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
package org.apache.ratis;

import org.apache.ratis.RaftTestUtil.SimpleMessage;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientRpc;
import org.apache.ratis.protocol.*;
import org.apache.ratis.protocol.exceptions.GroupMismatchException;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.protocol.exceptions.StaleReadException;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.raftlog.RaftLogIOException;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Slf4jUtils;
import org.apache.ratis.util.SizeInBytes;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.slf4j.event.Level;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

public abstract class RaftExceptionBaseTest<CLUSTER extends MiniRaftCluster>
    extends BaseTest
    implements MiniRaftCluster.Factory.Get<CLUSTER> {
  static {
    Slf4jUtils.setLogLevel(RaftServer.Division.LOG, Level.DEBUG);
    Slf4jUtils.setLogLevel(RaftLog.LOG, Level.DEBUG);
    Slf4jUtils.setLogLevel(RaftClient.LOG, Level.DEBUG);
  }

  static final int NUM_PEERS = 3;

  {
    RaftServerConfigKeys.Log.Appender.setBufferByteLimit(getProperties(), SizeInBytes.valueOf("4KB"));
  }

  @Test
  public void testHandleNotLeaderException() throws Exception {
    runWithNewCluster(NUM_PEERS, cluster -> runTestHandleNotLeaderException(cluster));
  }

  void runTestHandleNotLeaderException(CLUSTER cluster) throws Exception {
    final RaftPeerId oldLeader = RaftTestUtil.waitForLeader(cluster).getId();
    try(final RaftClient client = cluster.createClient(oldLeader)) {
      sendMessage("m1", client);

      // enforce leader change
      final RaftPeerId newLeader = RaftTestUtil.changeLeader(cluster, oldLeader);

      final RaftClientRpc rpc = client.getClientRpc();
      JavaUtils.attemptRepeatedly(() -> assertNotLeaderException(newLeader, "m2", oldLeader, rpc, cluster),
          10, ONE_SECOND, "assertNotLeaderException", LOG);

      sendMessage("m3", client);
    }
  }

  RaftClientReply assertNotLeaderException(RaftPeerId expectedSuggestedLeader,
      String messageId, RaftPeerId server, RaftClientRpc rpc, CLUSTER cluster) throws IOException {
    final SimpleMessage message = new SimpleMessage(messageId);
    final RaftClientReply reply = rpc.sendRequest(cluster.newRaftClientRequest(ClientId.randomId(), server, message));
    Assert.assertNotNull(reply);
    Assume.assumeFalse(reply.isSuccess());
    final NotLeaderException nle = reply.getNotLeaderException();
    Objects.requireNonNull(nle);
    Assert.assertEquals(expectedSuggestedLeader, nle.getSuggestedLeader().getId());
    return reply;
  }

  static void sendMessage(String message, RaftClient client) throws IOException {
    final RaftClientReply reply = client.io().send(new SimpleMessage(message));
    Assert.assertTrue(reply.isSuccess());
  }

  @Test
  public void testNotLeaderExceptionWithReconf() throws Exception {
    runWithNewCluster(NUM_PEERS, this::runTestNotLeaderExceptionWithReconf);
  }

  void runTestNotLeaderExceptionWithReconf(CLUSTER cluster) throws Exception {
    final RaftPeerId oldLeader = RaftTestUtil.waitForLeader(cluster).getId();
    try(final RaftClient client = cluster.createClient(oldLeader)) {

      // enforce leader change
      final RaftPeerId newLeader = RaftTestUtil.changeLeader(cluster, oldLeader);

      // add two more peers
      MiniRaftCluster.PeerChanges change = cluster.addNewPeers(new String[]{
          "ss1", "ss2"}, true, false);
      // trigger setConfiguration
      LOG.info("Start changing the configuration: {}", Arrays.asList(change.allPeersInNewConf));
      try (final RaftClient c2 = cluster.createClient(newLeader)) {
        RaftClientReply reply = c2.admin().setConfiguration(change.allPeersInNewConf);
        Assert.assertTrue(reply.isSuccess());
      }
      LOG.info(cluster.printServers());

      // it is possible that the remote peer's rpc server is not ready. need retry
      final RaftClientRpc rpc = client.getClientRpc();
      final RaftClientReply reply = JavaUtils.attemptRepeatedly(
          () -> assertNotLeaderException(newLeader, "m1", oldLeader, rpc, cluster),
          10, ONE_SECOND, "assertNotLeaderException", LOG);

      final Collection<RaftPeer> peers = cluster.getPeers();
      final Collection<RaftPeer> peersFromReply = reply.getNotLeaderException().getPeers();
      Assert.assertEquals(peers.size(), peersFromReply.size());
      for (RaftPeer p : peersFromReply) {
        Assert.assertTrue(peers.contains(p));
      }

      sendMessage("m2", client);
    }
  }

  @Test
  public void testGroupMismatchException() throws Exception {
    runWithSameCluster(NUM_PEERS, this::runTestGroupMismatchException);
  }

  void runTestGroupMismatchException(CLUSTER cluster) throws Exception {
    final RaftGroup clusterGroup = cluster.getGroup();
    Assert.assertEquals(NUM_PEERS, clusterGroup.getPeers().size());

    final RaftGroup anotherGroup = RaftGroup.valueOf(RaftGroupId.randomId(), clusterGroup.getPeers());
    Assert.assertNotEquals(clusterGroup.getGroupId(), anotherGroup.getGroupId());

    // Create client using another group
    try(RaftClient client = cluster.createClient(anotherGroup)) {
      testFailureCase("send(..) with client group being different from the server group",
          () -> client.io().send(Message.EMPTY),
          GroupMismatchException.class);

      testFailureCase("sendReadOnly(..) with client group being different from the server group",
          () -> client.io().sendReadOnly(Message.EMPTY),
          GroupMismatchException.class);

      testFailureCase("setConfiguration(..) with client group being different from the server group",
          () -> client.admin().setConfiguration(RaftPeer.emptyArray()),
          GroupMismatchException.class);

      testFailureCase("groupRemove(..) with another group id",
          () -> client.getGroupManagementApi(clusterGroup.getPeers().iterator().next().getId())
              .remove(anotherGroup.getGroupId(), false, false),
          GroupMismatchException.class);
    }
  }

  @Test
  public void testStaleReadException() throws Exception {
    runWithSameCluster(NUM_PEERS, this::runTestStaleReadException);
  }

  void runTestStaleReadException(CLUSTER cluster) throws Exception {
    RaftTestUtil.waitForLeader(cluster);
    try (RaftClient client = cluster.createClient()) {
      final RaftPeerId follower = cluster.getFollowers().iterator().next().getId();
      testFailureCase("sendStaleRead(..) with a large commit index",
          () -> client.io().sendStaleRead(Message.EMPTY, 1_000_000_000L, follower),
          StateMachineException.class, StaleReadException.class);
    }
  }

  @Test
  public void testLogAppenderBufferCapacity() throws Exception {
    runWithSameCluster(NUM_PEERS, this::runTestLogAppenderBufferCapacity);
  }

  void runTestLogAppenderBufferCapacity(CLUSTER cluster) throws Exception {
    final RaftPeerId leaderId = RaftTestUtil.waitForLeader(cluster).getId();
    byte[] bytes = new byte[8192];
    Arrays.fill(bytes, (byte) 1);
    SimpleMessage msg = new SimpleMessage(new String(bytes));
    try (RaftClient client = cluster.createClient(leaderId)) {
      testFailureCase("testLogAppenderBufferCapacity",
          () -> client.io().send(msg),
          StateMachineException.class, RaftLogIOException.class);
    }
  }
}
