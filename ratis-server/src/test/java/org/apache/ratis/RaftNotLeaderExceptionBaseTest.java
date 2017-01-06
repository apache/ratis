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
import org.apache.ratis.client.RaftClientRequestSender;
import org.apache.ratis.client.impl.RaftClientImpl;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.simulation.RequestHandler;
import org.apache.ratis.server.storage.RaftLog;
import org.apache.ratis.util.RaftUtils;
import org.junit.*;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.ratis.server.impl.RaftServerConstants.DEFAULT_SEQNUM;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

public abstract class RaftNotLeaderExceptionBaseTest {
  static {
    RaftUtils.setLogLevel(RaftServerImpl.LOG, Level.DEBUG);
    RaftUtils.setLogLevel(RaftLog.LOG, Level.DEBUG);
    RaftUtils.setLogLevel(RequestHandler.LOG, Level.DEBUG);
    RaftUtils.setLogLevel(RaftClient.LOG, Level.DEBUG);
  }

  public static final Logger LOG =
      LoggerFactory.getLogger(RaftNotLeaderExceptionBaseTest.class);
  public static final int NUM_PEERS = 3;

  @Rule
  public Timeout globalTimeout = new Timeout(60 * 1000);

  private MiniRaftCluster cluster;

  public abstract MiniRaftCluster initCluster() throws IOException;

  @Before
  public void setup() throws IOException {
    this.cluster = initCluster();
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
    RaftTestUtil.waitForLeader(cluster);
    final String leaderId = cluster.getLeader().getId();
    final RaftClient client = cluster.createClient("client", leaderId);

    RaftClientReply reply = client.send(new SimpleMessage("m1"));
    Assert.assertTrue(reply.isSuccess());

    // enforce leader change
    String newLeader = RaftTestUtil.changeLeader(cluster, leaderId);
    Assert.assertNotEquals(leaderId, newLeader);

    RaftClientRequestSender rpc = ((RaftClientImpl)client).getRequestSender();
    reply= null;
    for (int i = 0; reply == null && i < 10; i++) {
      try {
        reply = rpc.sendRequest(
            new RaftClientRequest("client", leaderId, DEFAULT_SEQNUM,
                new SimpleMessage("m2")));
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
    client.close();
  }

  @Test
  public void testNotLeaderExceptionWithReconf() throws Exception {
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
    try(final RaftClient c2 = cluster.createClient("client2", newLeader)) {
      RaftClientReply reply = c2.setConfiguration(change.allPeersInNewConf);
      Assert.assertTrue(reply.isSuccess());
    }
    LOG.info(cluster.printServers());

    RaftClientRequestSender rpc = ((RaftClientImpl)client).getRequestSender();
    RaftClientReply reply = null;
    // it is possible that the remote peer's rpc server is not ready. need retry
    for (int i = 0; reply == null && i < 10; i++) {
      try {
        reply = rpc.sendRequest(
            new RaftClientRequest("client", leaderId, DEFAULT_SEQNUM,
                new SimpleMessage("m1")));
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
    client.close();
  }
}
