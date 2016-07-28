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
package org.apache.hadoop.raft.server.simulation;

import org.apache.hadoop.raft.RaftBasicTests;
import org.apache.hadoop.raft.RaftTestUtil;
import org.apache.hadoop.raft.RaftTestUtil.*;
import org.apache.hadoop.raft.client.RaftClient;
import org.apache.hadoop.raft.server.RaftConstants;
import org.apache.hadoop.raft.server.RaftServer;
import org.apache.hadoop.raft.server.protocol.RaftServerReply;
import org.apache.hadoop.raft.server.protocol.RaftServerRequest;
import org.apache.hadoop.raft.server.storage.SegmentedRaftLog;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.raft.RaftTestUtil.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestRaftWithSimulatedRpc extends RaftBasicTests {
  static final Logger LOG = LoggerFactory.getLogger(TestRaftWithSimulatedRpc.class);

  static {
    GenericTestUtils.setLogLevel(RaftServer.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(SegmentedRaftLog.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(RequestHandler.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(RaftClient.LOG, Level.DEBUG);
  }

  private final MiniRaftClusterWithSimulatedRpc cluster;

  public TestRaftWithSimulatedRpc() throws IOException {
    cluster = new MiniRaftClusterWithSimulatedRpc(NUM_SERVERS, getProperties());
  }

  @Override
  public MiniRaftClusterWithSimulatedRpc getCluster() {
    return cluster;
  }

  @Test
  public void testEnforceLeader() throws Exception {
    waitForLeader(cluster);
    waitForLeader(cluster, "s0");
    assertEquals("s0", cluster.getLeader().getId());
  }

  @Test
  public void testWithLoad() throws Exception {
    LOG.info(cluster.printServers());

    final SimpleMessage[] messages = new SimpleMessage[500];
    final RaftClient client = cluster.createClient("client", null);
    for (int i = 0; i < messages.length; i++) {
      messages[i] = new SimpleMessage("m" + i);
    }
    final Exception[] exceptionInClientThread = new Exception[1];
    AtomicBoolean done = new AtomicBoolean(false);

    Thread clientThread = new Thread(() -> {
      try {
        for (SimpleMessage message : messages) {
          client.send(message);
        }
        done.set(true);
      } catch (IOException ioe) {
        LOG.error(ioe.toString());
        exceptionInClientThread[0] = ioe;
      }
    });
    clientThread.start();

    final SimulatedRequestReply<RaftServerRequest, RaftServerReply>
        serverRequestReply = cluster.getServerRequestReply();
    while (!done.get()) {
      Thread.sleep(2000);
      RaftServer leader = cluster.getLeader();
      if (leader != null) {
        String leaderId = leader.getId();
        List<RaftServer> followers = cluster.getFollowers();
        String[] followerIds = new String[followers.size()];
        for (int i = 0; i < followers.size(); i++) {
          followerIds[i] = followers.get(i).getId();
        }
        serverRequestReply.addBlacklist(leaderId, followerIds);
        Thread.sleep(RaftConstants.ELECTION_TIMEOUT_MAX_MS + 10);
        serverRequestReply.removeBlacklist(leaderId, followerIds);
      }
      LOG.info("Changed leader");
      LOG.info(cluster.printServers());
    }

    LOG.info(cluster.printAllLogs());
    clientThread.join();
    assertNull("Exception: " + exceptionInClientThread[0],
        exceptionInClientThread[0]);

    cluster.getServers().stream().filter(RaftServer::isRunning)
        .map(s -> s.getState().getLog().getEntries(0, Long.MAX_VALUE))
        .forEach(e -> RaftTestUtil.assertLogEntriesContains(e, messages));
  }
}
