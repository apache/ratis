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

import org.apache.hadoop.raft.server.RaftServer;
import org.apache.hadoop.raft.server.RaftConstants;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Test;

import java.io.PrintStream;

public class TestRaft {
  {
    GenericTestUtils.setLogLevel(RaftServer.LOG, Level.DEBUG);
  }
  static final PrintStream out = System.out;

  @Test
  public void testBasicLeaderElection() throws Exception {
    final MiniRaftCluster cluster = new MiniRaftCluster(5);
    Assert.assertNull(cluster.getLeader());

    waitAndKillLeader(cluster, true);
    waitAndKillLeader(cluster, true);
    waitAndKillLeader(cluster, true);
    waitAndKillLeader(cluster, false);
  }

  static void waitAndKillLeader(MiniRaftCluster cluster, boolean expectLeader)
      throws InterruptedException {
    cluster.printServers(out);
    Thread.sleep(RaftConstants.ELECTION_TIMEOUT_MAX_MS + 100);
    cluster.printServers(out);

    final RaftServer leader = cluster.getLeader();
    if (!expectLeader) {
      Assert.assertNull(leader);
    } else {
      Assert.assertNotNull(leader);
      out.println("leader = " + leader);
      cluster.killServer(leader.getState().getSelfId());
    }
  }
}
