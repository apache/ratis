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

import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.RetryCacheTests;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.exceptions.ResourceUnavailableException;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.RetryCacheTestUtil;
import org.apache.ratis.statemachine.SimpleStateMachine4Testing;
import org.apache.ratis.statemachine.StateMachine;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestRetryCacheWithGrpc
    extends RetryCacheTests<MiniRaftClusterWithGrpc>
    implements MiniRaftClusterWithGrpc.FactoryGet {

  @Test(timeout = 10000)
  public void testRetryOnResourceUnavailableException()
      throws InterruptedException, IOException {
    RaftProperties properties = new RaftProperties();
    properties.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        SimpleStateMachine4Testing.class, StateMachine.class);
    RaftServerConfigKeys.Write.setElementLimit(properties, 1);
    MiniRaftClusterWithGrpc cluster = getFactory().newCluster(NUM_SERVERS, properties);
    cluster.start();

    final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster);
    final RaftServer leaderProxy = leader.getRaftServer();
    for (RaftServer.Division follower : cluster.getFollowers()) {
      // block followers to trigger ResourceUnavailableException
      ((SimpleStateMachine4Testing) follower.getStateMachine()).blockWriteStateMachineData();
    }
    AtomicBoolean failure = new AtomicBoolean(false);
    long callId = 1;
    ClientId clientId = ClientId.randomId();
    RaftClientRequest r = null;
    while (!failure.get()) {
      long cid = callId;
      r = cluster.newRaftClientRequest(clientId, leaderProxy.getId(), callId++,
          new RaftTestUtil.SimpleMessage("message"));
      CompletableFuture<RaftClientReply> f = leaderProxy.submitClientRequestAsync(r);
      f.exceptionally(e -> {
        if (e.getCause() instanceof ResourceUnavailableException) {
          RetryCacheTestUtil.isFailed(RetryCacheTestUtil.get(leader, clientId, cid));
          failure.set(true);
        }
        return null;
      });
    }
    for (RaftServer.Division follower : cluster.getFollowers()) {
      // unblock followers
      ((SimpleStateMachine4Testing)follower.getStateMachine()).unblockWriteStateMachineData();
    }

    while (failure.get()) {
      try {
        // retry until the request failed with ResourceUnavailableException succeeds.
        RaftClientReply reply = leaderProxy.submitClientRequestAsync(r).get();
        if (reply.isSuccess()) {
          failure.set(false);
        }
      } catch (Exception e) {
        // Ignore the exception
      }
    }
    cluster.shutdown();
  }
}
