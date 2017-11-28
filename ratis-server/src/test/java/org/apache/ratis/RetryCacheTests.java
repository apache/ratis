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

import org.apache.ratis.MiniRaftCluster.PeerChanges;
import org.apache.ratis.RaftTestUtil.SimpleMessage;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientRpc;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.impl.RaftServerTestUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static java.util.Arrays.asList;

public abstract class RetryCacheTests extends BaseTest {
  public static final int NUM_SERVERS = 3;
  protected static final RaftProperties properties = new RaftProperties();

  public abstract MiniRaftCluster getCluster();

  public RaftProperties getProperties() {
    return properties;
  }

  @Before
  public void setup() throws IOException {
    Assert.assertNull(getCluster().getLeader());
    getCluster().start();
  }

  @After
  public void tearDown() {
    final MiniRaftCluster cluster = getCluster();
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * make sure the retry cache can correct capture the retry from a client,
   * and returns the result from the previous request
   */
  @Test
  public void testBasicRetry() throws Exception {
    final MiniRaftCluster cluster = getCluster();
    RaftTestUtil.waitForLeader(cluster);

    final RaftPeerId leaderId = cluster.getLeaderAndSendFirstMessage().getId();
    long oldLastApplied = cluster.getLeader().getState().getLastAppliedIndex();

    final RaftClient client = cluster.createClient(leaderId);
    final RaftClientRpc rpc = client.getClientRpc();
    final long callId = 999;
    final long seqNum = 111;
    RaftClientRequest r = new RaftClientRequest(client.getId(), leaderId,
        cluster.getGroupId(), callId, seqNum, new SimpleMessage("message"));
    RaftClientReply reply = rpc.sendRequest(r);
    Assert.assertEquals(callId, reply.getCallId());
    Assert.assertTrue(reply.isSuccess());

    // retry with the same callId
    for (int i = 0; i < 5; i++) {
      reply = rpc.sendRequest(r);
      Assert.assertEquals(client.getId(), reply.getClientId());
      Assert.assertEquals(callId, reply.getCallId());
      Assert.assertTrue(reply.isSuccess());
    }

    long leaderApplied = cluster.getLeader().getState().getLastAppliedIndex();
    // make sure retry cache has the entry
    for (RaftServerImpl server : cluster.iterateServerImpls()) {
      LOG.info("check server " + server.getId());
      if (server.getState().getLastAppliedIndex() < leaderApplied) {
        Thread.sleep(1000);
      }
      Assert.assertEquals(2, RaftServerTestUtil.getRetryCacheSize(server));
      Assert.assertNotNull(
          RaftServerTestUtil.getRetryEntry(server, client.getId(), callId));
      // make sure there is only one log entry committed
      Assert.assertEquals(oldLastApplied + 1,
          server.getState().getLastAppliedIndex());
    }
    client.close();
  }

  /**
   * Test retry while the leader changes to another peer
   */
  @Test
  public void testRetryOnNewLeader() throws Exception {
    final MiniRaftCluster cluster = getCluster();
    RaftTestUtil.waitForLeader(cluster);

    final RaftPeerId leaderId = cluster.getLeaderAndSendFirstMessage().getId();

    final RaftClient client = cluster.createClient(leaderId);
    RaftClientRpc rpc = client.getClientRpc();
    final long callId = 999;
    final long seqNum = 111;
    RaftClientRequest r = new RaftClientRequest(client.getId(), leaderId,
        cluster.getGroupId(), callId, seqNum, new SimpleMessage("message"));
    RaftClientReply reply = rpc.sendRequest(r);
    Assert.assertEquals(callId, reply.getCallId());
    Assert.assertTrue(reply.isSuccess());
    long oldLastApplied = cluster.getLeader().getState().getLastAppliedIndex();

    // trigger the reconfiguration, make sure the original leader is kicked out
    PeerChanges change = cluster.addNewPeers(2, true);
    RaftPeer[] allPeers = cluster.removePeers(2, true,
        asList(change.newPeers)).allPeersInNewConf;
    // trigger setConfiguration
    cluster.setConfiguration(allPeers);

    RaftTestUtil.waitForLeader(cluster);
    final RaftPeerId newLeaderId = cluster.getLeader().getId();
    Assert.assertNotEquals(leaderId, newLeaderId);
    // same clientId and callId in the request
    r = new RaftClientRequest(client.getId(), newLeaderId, cluster.getGroupId(),
        callId, seqNum, new SimpleMessage("message"));
    for (int i = 0; i < 10; i++) {
      try {
        reply = rpc.sendRequest(r);
        LOG.info("successfully sent out the retry request_" + i);
        Assert.assertEquals(client.getId(), reply.getClientId());
        Assert.assertEquals(callId, reply.getCallId());
        Assert.assertTrue(reply.isSuccess());
      } catch (Exception e) {
        LOG.info("hit exception while retrying the same request: " + e);
      }
      Thread.sleep(100);
    }

    // check the new leader and make sure the retry did not get committed
    Assert.assertEquals(oldLastApplied + 3,
        cluster.getLeader().getState().getLastAppliedIndex());
    client.close();
  }
}
