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

package org.apache.ratis.datastream;

import org.apache.ratis.MiniRaftCluster;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.MiniRaftClusterWithGrpc;
import org.apache.ratis.netty.server.NettyServerStreamRpc;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.statemachine.SimpleStateMachine4Testing;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.TimeDuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class TestDataStreamWithCluster implements MiniRaftClusterWithGrpc.FactoryGet {
  @Before
  public void setup() {
    final RaftProperties p = getProperties();
    p.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY, SimpleStateMachine4Testing.class, StateMachine.class);
    RaftClientConfigKeys.Rpc.setRequestTimeout(p, TimeDuration.valueOf(1, TimeUnit.SECONDS));
  }

  @Test
  public void testDataStreamServerSubmitClientRequest() throws Exception {
    runWithNewCluster(3, this::runTestDataStreamServerSubmitClientRequest);
  }

  void runTestDataStreamServerSubmitClientRequest(MiniRaftClusterWithGrpc cluster) throws Exception {
    final RaftServerImpl leader = RaftTestUtil.waitForLeader(cluster);
    NettyServerStreamRpc  serverStreamRpc = (NettyServerStreamRpc) leader.getProxy().getDataStreamServerRpc();
    ClientId clientId = ClientId.randomId();
    RaftClientRequest r = cluster.newRaftClientRequest(clientId, leader.getId(), 0,
        new RaftTestUtil.SimpleMessage("message"));
    CompletableFuture<RaftClientReply> f = serverStreamRpc.submitClientRequestAsync(r);
    try {
      Assert.assertTrue(f.get().isSuccess());
    } catch (Throwable t) {
      Assert.assertTrue(f.get().getNotLeaderException() != null);
    }
  }

}
