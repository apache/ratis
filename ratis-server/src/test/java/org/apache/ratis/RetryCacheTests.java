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

import org.apache.log4j.Level;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.server.impl.MiniRaftCluster.PeerChanges;
import org.apache.ratis.RaftTestUtil.SimpleMessage;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientRpc;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.impl.RetryCacheTestUtil;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.raftlog.RaftLogIOException;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Log4jUtils;
import org.apache.ratis.util.TimeDuration;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;

public abstract class RetryCacheTests<CLUSTER extends MiniRaftCluster>
    extends BaseTest
    implements MiniRaftCluster.Factory.Get<CLUSTER> {
  static {
    Log4jUtils.setLogLevel(RaftServer.Division.LOG, Level.DEBUG);
  }

  public static final int NUM_SERVERS = 3;

  /**
   * make sure the retry cache can correct capture the retry from a client,
   * and returns the result from the previous request
   */
  @Test
  public void testBasicRetry() throws Exception {
    runWithNewCluster(NUM_SERVERS, this::runTestBasicRetry);
  }

  void runTestBasicRetry(CLUSTER cluster) throws Exception {
    RaftTestUtil.waitForLeader(cluster);
    final RaftPeerId leaderId = cluster.getLeaderAndSendFirstMessage(false).getId();
    final long oldLastApplied = cluster.getLeader().getInfo().getLastAppliedIndex();

    try (final RaftClient client = cluster.createClient(leaderId)) {
      final RaftClientRpc rpc = client.getClientRpc();
      final long callId = 999;
      RaftClientRequest r = cluster.newRaftClientRequest(client.getId(), leaderId,
              callId, new SimpleMessage("message"));
      assertReply(rpc.sendRequest(r), client, callId);

      // retry with the same callId
      for (int i = 0; i < 5; i++) {
        assertReply(rpc.sendRequest(r), client, callId);
      }

      assertServer(cluster, client.getId(), callId, oldLastApplied);
    }
  }

  public static RaftClient assertReply(RaftClientReply reply, RaftClient client, long callId) {
    Assert.assertEquals(client.getId(), reply.getClientId());
    Assert.assertEquals(callId, reply.getCallId());
    Assert.assertTrue(reply.isSuccess());
    return client;
  }

  public void assertServer(MiniRaftCluster cluster, ClientId clientId, long callId, long oldLastApplied) throws Exception {
    final long leaderApplied = cluster.getLeader().getInfo().getLastAppliedIndex();
    // make sure retry cache has the entry
    for (RaftServer.Division server : cluster.iterateDivisions()) {
      LOG.info("check server " + server.getId());
      if (server.getInfo().getLastAppliedIndex() < leaderApplied) {
        Thread.sleep(1000);
      }
      Assert.assertEquals(2, server.getRetryCache().getStatistics().size());
      Assert.assertNotNull(RetryCacheTestUtil.get(server, clientId, callId));
      // make sure there is only one log entry committed
      Assert.assertEquals(1, count(server.getRaftLog(), oldLastApplied + 1));
    }
  }

  static int count(RaftLog log, long startIndex) throws RaftLogIOException {
    final long nextIndex = log.getNextIndex();
    int count = 0;
    for(long i = startIndex; i < nextIndex; i++) {
      if (log.get(i).hasStateMachineLogEntry()) {
        count++;
      }
    }
    return count;
  }

  /**
   * Test retry while the leader changes to another peer
   */
  @Test
  public void testRetryOnNewLeader() throws Exception {
    runWithNewCluster(NUM_SERVERS, this::runTestRetryOnNewLeader);
  }

  void runTestRetryOnNewLeader(CLUSTER cluster) throws Exception {
    RaftTestUtil.waitForLeader(cluster);
    final RaftPeerId leaderId = cluster.getLeaderAndSendFirstMessage(false).getId();

    try (final RaftClient client = cluster.createClient(leaderId)) {
      RaftClientRpc rpc = client.getClientRpc();
      final long callId = 999;
      RaftClientRequest r = cluster.newRaftClientRequest(client.getId(), leaderId,
              callId, new SimpleMessage("message"));
      assertReply(rpc.sendRequest(r), client, callId);
      final long oldLastApplied = cluster.getLeader().getInfo().getLastAppliedIndex();

      // trigger the reconfiguration, make sure the original leader is kicked out
      PeerChanges change = cluster.addNewPeers(2, true);
      RaftPeer[] allPeers = cluster.removePeers(2, true,
              asList(change.newPeers)).allPeersInNewConf;
      // trigger setConfiguration
      cluster.setConfiguration(allPeers);

      final RaftPeerId newLeaderId = JavaUtils.attemptRepeatedly(() -> {
        final RaftPeerId id = RaftTestUtil.waitForLeader(cluster).getId();
        Assert.assertNotEquals(leaderId, id);
        return id;
      }, 10, TimeDuration.valueOf(100, TimeUnit.MILLISECONDS), "wait for a leader different than " + leaderId, LOG);
      Assert.assertNotEquals(leaderId, newLeaderId);
      // same clientId and callId in the request
      r = cluster.newRaftClientRequest(client.getId(), newLeaderId,
              callId, new SimpleMessage("message"));
      rpc.addRaftPeers(Arrays.asList(change.newPeers));
      for (int i = 0; i < 10; i++) {
        try {
          assertReply(rpc.sendRequest(r), client, callId);
          LOG.info("successfully sent out the retry request_" + i);
        } catch (Exception e) {
          LOG.info("hit exception while retrying the same request: " + r, e);
        }
        Thread.sleep(100);
      }

      // check the new leader and make sure the retry did not get committed
      Assert.assertEquals(0, count(cluster.getLeader().getRaftLog(), oldLastApplied + 1));
    }
  }
}
