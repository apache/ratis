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
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.util.LogUtils;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.examples.RaftExamplesTestUtil;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.simulation.RequestHandler;
import org.apache.ratis.server.storage.RaftLog;
import org.apache.ratis.statemachine.SimpleStateMachine4Testing;
import org.apache.ratis.statemachine.StateMachine;
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

/**
 * Test restarting raft peers.
 */
@RunWith(Parameterized.class)
public class TestRestartRaftPeer {
  static Logger LOG = LoggerFactory.getLogger(TestRestartRaftPeer.class);
  static {
    LogUtils.setLogLevel(RaftServerImpl.LOG, Level.DEBUG);
    LogUtils.setLogLevel(RaftLog.LOG, Level.DEBUG);
    LogUtils.setLogLevel(RequestHandler.LOG, Level.DEBUG);
    LogUtils.setLogLevel(RaftClient.LOG, Level.DEBUG);
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() throws IOException {
    RaftProperties prop = new RaftProperties();
    prop.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        SimpleStateMachine4Testing.class, StateMachine.class);
    RaftServerConfigKeys.Log.setSegmentSizeMax(prop, SizeInBytes.valueOf("8KB"));
    return RaftExamplesTestUtil.getMiniRaftClusters(prop, 3);
  }

  @Parameterized.Parameter
  public MiniRaftCluster cluster;

  @Rule
  public Timeout globalTimeout = new Timeout(60 * 1000);

  @Test
  public void restartFollower() throws Exception {
    cluster.start();
    RaftTestUtil.waitForLeader(cluster);
    final RaftPeerId leaderId = cluster.getLeader().getId();
    final RaftClient client = cluster.createClient(leaderId);

    // write some messages
    final byte[] content = new byte[1024];
    Arrays.fill(content, (byte) 1);
    final SimpleMessage message = new SimpleMessage(new String(content));
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(client.send(message).isSuccess());
    }

    // restart a follower
    String followerId = cluster.getFollowers().get(0).getId().toString();
    LOG.info("Restart follower {}", followerId);
    cluster.restartServer(followerId, false);

    // write some more messages
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(client.send(message).isSuccess());
    }
    client.close();

    // make sure the restarted follower can catchup
    boolean catchup = false;
    long lastAppliedIndex = 0;
    for (int i = 0; i < 10 && !catchup; i++) {
      Thread.sleep(500);
      lastAppliedIndex = cluster.getServer(followerId).getState().getLastAppliedIndex();
      catchup = lastAppliedIndex >= 20;
    }
    Assert.assertTrue("lastAppliedIndex=" + lastAppliedIndex, catchup);

    // make sure the restarted peer's log segments is correct
    cluster.restartServer(followerId, false);
    Assert.assertTrue(cluster.getServer(followerId).getState().getLog()
        .getLastEntry().getIndex() >= 20);
  }
}
