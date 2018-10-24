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
package org.apache.ratis.server;

import org.apache.log4j.Level;
import org.apache.ratis.BaseTest;
import org.apache.ratis.MiniRaftCluster;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.RaftTestUtil.SimpleMessage;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.impl.ServerState;
import org.apache.ratis.server.storage.RaftLog;
import org.apache.ratis.statemachine.SimpleStateMachine4Testing;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.LogUtils;
import org.apache.ratis.util.SizeInBytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Test restarting raft peers.
 */
public abstract class ServerRestartTests<CLUSTER extends MiniRaftCluster>
    extends BaseTest
    implements MiniRaftCluster.Factory.Get<CLUSTER> {
  static {
    LogUtils.setLogLevel(RaftServerImpl.LOG, Level.DEBUG);
    LogUtils.setLogLevel(RaftLog.LOG, Level.DEBUG);
  }

  static final int NUM_SERVERS = 3;

  @Before
  public void setup() {
    final RaftProperties prop = getProperties();
    prop.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        SimpleStateMachine4Testing.class, StateMachine.class);
    RaftServerConfigKeys.Log.setSegmentSizeMax(prop, SizeInBytes.valueOf("8KB"));
  }

  @Test
  public void testRestartFollower() throws Exception {
    try(final MiniRaftCluster cluster = newCluster(NUM_SERVERS)) {
      runTestRestartFollower(cluster, LOG);
    }
  }

  static void runTestRestartFollower(MiniRaftCluster cluster, Logger LOG) throws Exception {
    cluster.start();
    RaftTestUtil.waitForLeader(cluster);
    final RaftPeerId leaderId = cluster.getLeader().getId();
    final RaftClient client = cluster.createClient(leaderId);

    // write some messages
    final byte[] content = new byte[1024];
    Arrays.fill(content, (byte)1);
    final SimpleMessage message = new SimpleMessage(new String(content));
    for(int i = 0; i < 10; i++) {
      Assert.assertTrue(client.send(message).isSuccess());
    }

    // restart a follower
    RaftPeerId followerId = cluster.getFollowers().get(0).getId();
    LOG.info("Restart follower {}", followerId);
    cluster.restartServer(followerId, false);

    // write some more messages
    for(int i = 0; i < 10; i++) {
      Assert.assertTrue(client.send(message).isSuccess());
    }
    client.close();

    final long leaderLastIndex = cluster.getLeader().getState().getLog().getLastEntryTermIndex().getIndex();
    // make sure the restarted follower can catchup
    final ServerState followerState = cluster.getRaftServerImpl(followerId).getState();
    JavaUtils.attempt(() -> followerState.getLastAppliedIndex() >= leaderLastIndex,
        10, 500, "follower catchup", LOG);

    // make sure the restarted peer's log segments is correct
    cluster.restartServer(followerId, false);
    Assert.assertTrue(cluster.getRaftServerImpl(followerId).getState().getLog()
        .getLastEntryTermIndex().getIndex() >= leaderLastIndex);
  }
}
