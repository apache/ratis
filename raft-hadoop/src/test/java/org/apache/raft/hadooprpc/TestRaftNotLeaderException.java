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
package org.apache.raft.hadooprpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.apache.raft.MiniRaftCluster;
import org.apache.raft.RaftTestUtil;
import org.apache.raft.RaftTestUtil.SimpleMessage;
import org.apache.raft.client.RaftClient;
import org.apache.raft.client.RaftClientRequestSender;
import org.apache.raft.conf.RaftProperties;
import org.apache.raft.protocol.RaftClientReply;
import org.apache.raft.protocol.RaftClientRequest;
import org.apache.raft.protocol.RaftPeer;
import org.apache.raft.server.RaftServer;
import org.apache.raft.server.RaftServerConfigKeys;
import org.apache.raft.server.simulation.RequestHandler;
import org.apache.raft.server.storage.RaftLog;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class TestRaftNotLeaderException {
  static {
    GenericTestUtils.setLogLevel(RaftServer.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(RaftLog.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(RequestHandler.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(RaftClient.LOG, Level.DEBUG);
  }

  static final Logger LOG = LoggerFactory.getLogger(TestRaftNotLeaderException.class);

  @Parameterized.Parameters
  public static Collection<Object[]> data() throws IOException {
    final Configuration conf = new Configuration();
    conf.set(RaftServerConfigKeys.Ipc.ADDRESS_KEY, "0.0.0.0:0");
    RaftProperties prop = new RaftProperties();
    return RaftHadoopRpcTestUtil.getMiniRaftClusters(3, conf, prop);
  }

  @Parameterized.Parameter
  public MiniRaftCluster cluster;

  @Rule
  public Timeout globalTimeout = new Timeout(60 * 1000);

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testHandleNotLeaderException() throws Exception {
    cluster.start();
    RaftTestUtil.waitForLeader(cluster);
    final String leaderId = cluster.getLeader().getId();
    final RaftClient client = cluster.createClient("client", leaderId);

    RaftClientReply reply = client.send(new SimpleMessage("m1"));
    Assert.assertTrue(reply.isSuccess());

    // enforce leader change
    String newLeader = RaftTestUtil.changeLeader(cluster, leaderId);
    Assert.assertNotEquals(leaderId, newLeader);

    RaftClientRequestSender rpc = client.getRequestSender();
    reply= null;
    for (int i = 0; reply == null && i < 10; i++) {
      try {
        reply = rpc.sendRequest(
            new RaftClientRequest("client", leaderId, new SimpleMessage("m2")));
      } catch (IOException ignored) {
        Thread.sleep(1000);
      }
    }
    Assert.assertNotNull(reply);
    Assert.assertFalse(reply.isSuccess());
    Assert.assertTrue(reply.isNotLeader());
    Assert.assertEquals(newLeader,
        reply.getNotLeaderException().getSuggestedLeader().getId());

    reply = client.send(new SimpleMessage("m3"));
    Assert.assertTrue(reply.isSuccess());
  }

  @Test
  public void testNotLeaderExceptionWithReconf() throws Exception {
    LOG.info("restart the " + cluster.getClass().getSimpleName()
        + " for testNotLeaderExceptionWithReconf");
    cluster.restart(true);
    Assert.assertNotNull(RaftTestUtil.waitForLeader(cluster));

    final String leaderId = cluster.getLeader().getId();
    final RaftClient client = cluster.createClient("client", leaderId);

    // enforce leader change
    String newLeader = RaftTestUtil.changeLeader(cluster, leaderId);
    Assert.assertNotEquals(leaderId, newLeader);

    // also add two new peers
    // add two more peers
    MiniRaftCluster.PeerChanges change = cluster.addNewPeers(
        new String[]{"ss1", "ss2"}, true);
    // trigger setConfiguration
    LOG.info("Start changing the configuration: {}",
        Arrays.asList(change.allPeersInNewConf));
    RaftClientReply reply = cluster.createClient("client2", newLeader)
        .setConfiguration(change.allPeersInNewConf);
    Assert.assertTrue(reply.isSuccess());
    LOG.info(cluster.printServers());

    RaftClientRequestSender rpc = client.getRequestSender();
    reply = null;
    // it is possible that the remote peer's rpc server is not ready. need retry
    for (int i = 0; reply == null && i < 10; i++) {
      try {
        reply = rpc.sendRequest(
            new RaftClientRequest("client", leaderId, new SimpleMessage("m1")));
      } catch (IOException ignored) {
        Thread.sleep(1000);
      }
    }
    Assert.assertNotNull(reply);
    Assert.assertFalse(reply.isSuccess());
    Assert.assertTrue(reply.isNotLeader());
    Assert.assertEquals(newLeader,
        reply.getNotLeaderException().getSuggestedLeader().getId());
    Collection<RaftPeer> peers = cluster.getPeers();
    RaftPeer[] peersFromReply = reply.getNotLeaderException().getPeers();
    Assert.assertEquals(peers.size(), peersFromReply.length);
    for (RaftPeer p : peersFromReply) {
      Assert.assertTrue(peers.contains(p));
    }

    reply = client.send(new SimpleMessage("m2"));
    Assert.assertTrue(reply.isSuccess());
  }
}
