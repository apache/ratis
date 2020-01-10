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
package org.apache.ratis.grpc;

import org.apache.ratis.LogAppenderTests;
import org.apache.ratis.MiniRaftCluster;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.statemachine.SimpleStateMachine4Testing;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.JavaUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static org.apache.ratis.RaftTestUtil.waitForLeader;

public class TestLogAppenderWithGrpc
    extends LogAppenderTests<MiniRaftClusterWithGrpc>
    implements MiniRaftClusterWithGrpc.FactoryGet {

  @Test
  public void testPendingLimits() throws IOException, InterruptedException {
    int maxAppends = 10;
    RaftProperties properties = new RaftProperties();
    properties.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        SimpleStateMachine4Testing.class, StateMachine.class);
    GrpcConfigKeys.Server.setLeaderOutstandingAppendsMax(properties, maxAppends);
    RaftServerConfigKeys.Log.Appender.setBufferElementLimit(properties, 1);
    MiniRaftClusterWithGrpc cluster = getFactory().newCluster(3, properties);
    cluster.start();

    // client and leader setup
    RaftClient client = cluster.createClient(cluster.getGroup());
    client.send(new RaftTestUtil.SimpleMessage("m"));
    RaftServerImpl leader = waitForLeader(cluster);
    long initialNextIndex = leader.getState().getNextIndex();

    for (RaftServerImpl server : cluster.getFollowers()) {
      // block the appends in the follower
      ((SimpleStateMachine4Testing)server.getStateMachine()).blockWriteStateMachineData();
    }
    Collection<CompletableFuture<RaftClientReply>> futures = new ArrayList<>(maxAppends * 2);
    for (int i = 0; i < maxAppends * 2; i++) {
      futures.add(client.sendAsync(new RaftTestUtil.SimpleMessage("m")));
    }

    FIVE_SECONDS.sleep();
    for (long nextIndex : leader.getFollowerNextIndices()) {
      // Verify nextIndex does not progress due to pendingRequests limit
      Assert.assertEquals(initialNextIndex + maxAppends, nextIndex);
    }
    ONE_SECOND.sleep();
    for (RaftServerImpl server : cluster.getFollowers()) {
      // unblock the appends in the follower
      ((SimpleStateMachine4Testing)server.getStateMachine()).unblockWriteStateMachineData();
    }

    JavaUtils.allOf(futures).join();
    cluster.shutdown();
  }
}
