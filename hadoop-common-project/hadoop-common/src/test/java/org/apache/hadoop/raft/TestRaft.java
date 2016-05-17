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
package org.apache.hadoop.raft;

import org.apache.hadoop.raft.RaftTestUtil.SimpleMessage;
import org.apache.hadoop.raft.client.RaftClient;
import org.apache.hadoop.raft.server.RaftConstants;
import org.apache.hadoop.raft.server.RaftLog;
import org.apache.hadoop.raft.server.RaftServer;
import org.apache.hadoop.raft.server.RequestHandler;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Test;

import java.io.PrintStream;

import static org.apache.hadoop.raft.RaftTestUtil.assertLogEntries;
import static org.apache.hadoop.raft.RaftTestUtil.waitAndKillLeader;
import static org.apache.hadoop.raft.RaftTestUtil.waitForLeader;

public class TestRaft {
  static {
    GenericTestUtils.setLogLevel(RaftServer.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(RaftLog.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(RequestHandler.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(RaftClient.LOG, Level.DEBUG);
  }
  static final PrintStream out = System.out;

  @Test
  public void testBasicLeaderElection() throws Exception {
    final MiniRaftCluster cluster = new MiniRaftCluster(5);
    Assert.assertNull(cluster.getLeader());
    cluster.start();

    waitAndKillLeader(cluster, true);
    waitAndKillLeader(cluster, true);
    waitAndKillLeader(cluster, true);
    waitAndKillLeader(cluster, false);
  }

  @Test
  public void testBasicAppendEntries() throws Exception {
    final MiniRaftCluster cluster = new MiniRaftCluster(5);
    cluster.start();
    RaftServer leader = waitForLeader(cluster);
    final long term = leader.getState().getCurrentTerm();
    final String killed = cluster.getFollowers().get(3).getId();
    cluster.killServer(killed);
    cluster.printServers(out);

    final SimpleMessage[] messages = new SimpleMessage[10];
    final RaftClient client = cluster.createClient("client", null);
    for(int i = 0; i < messages.length; i++) {
      messages[i] = new SimpleMessage("m" + i);
      client.send(messages[i]);
    }

    Thread.sleep(RaftConstants.ELECTION_TIMEOUT_MAX_MS + 100);
    cluster.printAllLogs(out);

    for(RaftServer s : cluster.getServers()) {
      if (s.isRunning()) {
        assertLogEntries(s.getState().getLog().getEntries(2), 2, term, messages);
      }
    }
  }
}
