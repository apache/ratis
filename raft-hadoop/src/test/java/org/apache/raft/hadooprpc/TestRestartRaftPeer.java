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
import org.apache.raft.conf.RaftProperties;
import org.apache.raft.server.RaftServer;
import org.apache.raft.server.RaftServerConfigKeys;
import org.apache.raft.statemachine.SimpleStateMachine;
import org.apache.raft.statemachine.StateMachine;
import org.apache.raft.server.simulation.RequestHandler;
import org.apache.raft.server.storage.RaftLog;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

/**
 * Test restarting raft peers.
 */
@RunWith(Parameterized.class)
public class TestRestartRaftPeer {
  static {
    GenericTestUtils.setLogLevel(RaftServer.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(RaftLog.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(RequestHandler.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(RaftClient.LOG, Level.DEBUG);
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() throws IOException {
    final Configuration conf = new Configuration();
    conf.set(RaftServerConfigKeys.Ipc.ADDRESS_KEY, "0.0.0.0:0");

    RaftProperties prop = new RaftProperties();
    prop.setClass(RaftServerConfigKeys.RAFT_SERVER_STATEMACHINE_CLASS_KEY,
        SimpleStateMachine.class, StateMachine.class);
    prop.setInt(RaftServerConfigKeys.RAFT_LOG_SEGMENT_MAX_SIZE_KEY, 1024 * 8);
    return RaftHadoopRpcTestUtil.getMiniRaftClusters(3, conf, prop);
  }

  @Parameterized.Parameter
  public MiniRaftCluster cluster;

  @Rule
  public Timeout globalTimeout = new Timeout(60 * 1000);

  @Test
  public void restartFollower() throws Exception {
    cluster.start();
    RaftTestUtil.waitForLeader(cluster);
    final String leaderId = cluster.getLeader().getId();
    final RaftClient client = cluster.createClient("client", leaderId);

    // write some messages
    final byte[] content = new byte[1024];
    Arrays.fill(content, (byte) 1);
    final SimpleMessage message = new SimpleMessage(new String(content));
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(client.send(message).isSuccess());
    }

    // restart a follower
    String followerId = cluster.getFollowers().get(0).getId();
    cluster.restartServer(followerId, false);

    // write some more messages
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(client.send(message).isSuccess());
    }

    // make sure the restarted follower can catchup
    boolean catchup = false;
    for (int i = 0; i < 10 && !catchup; i++) {
      Thread.sleep(500);
      catchup =
          cluster.getServer(followerId).getState().getLastAppliedIndex() >= 20;
    }
    Assert.assertTrue(catchup);

    // make sure the restarted peer's log segments is correct
    cluster.restartServer(followerId, false);
    Assert.assertTrue(cluster.getServer(followerId).getState().getLog()
        .getLastEntry().getIndex() >= 20);
  }

}
