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
package org.apache.ratis;

import org.apache.log4j.Level;
import org.apache.ratis.RaftTestUtil.SimpleMessage;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.ReplicationLevel;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.exceptions.ResourceUnavailableException;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.server.impl.RaftServerTestUtil;
import org.apache.ratis.statemachine.SimpleStateMachine4Testing;
import org.apache.ratis.util.Log4jUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public abstract class RequestLimitAsyncBaseTest<CLUSTER extends MiniRaftCluster>
    extends BaseTest
    implements MiniRaftCluster.Factory.Get<CLUSTER> {
  static {
    Log4jUtils.setLogLevel(RaftServer.Division.LOG, Level.DEBUG);
    Log4jUtils.setLogLevel(RaftClient.LOG, Level.DEBUG);
    RaftServerTestUtil.setPendingRequestsLogLevel(Level.DEBUG);
  }

  private final int writeElementLimit = 5;
  private final int watchElementLimit = 2;
  {
    final RaftProperties p = setStateMachine(SimpleStateMachine4Testing.class);
    RaftServerConfigKeys.Write.setElementLimit(p, writeElementLimit);
    RaftServerConfigKeys.Watch.setElementLimit(p, watchElementLimit);

    RaftServerConfigKeys.Rpc.setRequestTimeout(p, FIVE_SECONDS);
    RaftClientConfigKeys.Rpc.setRequestTimeout(p, FIVE_SECONDS);
  }

  @Test
  public void testWriteElementLimit() throws Exception {
    runWithSameCluster(1, this::runTestWriteElementLimit);
  }

  void runTestWriteElementLimit(CLUSTER cluster) throws Exception {
    final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster);

    try (RaftClient c1 = cluster.createClient(leader.getId())) {
      { // send first message to make sure the cluster is working
        final SimpleMessage message = new SimpleMessage("first");
        final CompletableFuture<RaftClientReply> future = c1.async().send(message);
        final RaftClientReply reply = getWithDefaultTimeout(future);
        Assert.assertTrue(reply.isSuccess());
      }

      // collecting futures returned from StateMachine.applyTransaction
      final BlockingQueue<Runnable> toBeCompleted = SimpleStateMachine4Testing.get(leader).collecting().enable(
          SimpleStateMachine4Testing.Collecting.Type.APPLY_TRANSACTION);

      // send write requests up to the limit
      final List<CompletableFuture<RaftClientReply>> writeFutures = new ArrayList<>();
      for (int i = 0; i < writeElementLimit; i++) {
        final SimpleMessage message = new SimpleMessage("m" + i);
        writeFutures.add(c1.async().send(message));
      }

      // send watch requests up to the limit
      final long watchBase = 1000; //watch a large index so that it won't complete
      for (int i = 0; i < watchElementLimit; i++) {
        c1.async().watch(watchBase + i, ReplicationLevel.ALL);
      }

      // sleep to make sure that all the request were sent
      HUNDRED_MILLIS.sleep();

      try(RaftClient c2 = cluster.createClient(leader.getId(), RetryPolicies.noRetry())) {
        // more write requests should get ResourceUnavailableException
        final SimpleMessage message = new SimpleMessage("err");
        testFailureCase("send should fail", () -> c2.io().send(message),
            ResourceUnavailableException.class);
        testFailureCase("sendAsync should fail", () -> c2.async().send(message).get(),
            ExecutionException.class, ResourceUnavailableException.class);

        // more watch requests should get ResourceUnavailableException
        final long watchIndex = watchBase + watchElementLimit;
        testFailureCase("sendWatch should fail", () -> c2.io().watch(watchIndex, ReplicationLevel.ALL),
            ResourceUnavailableException.class);
        testFailureCase("sendWatchAsync should fail", () -> c2.async().watch(watchIndex, ReplicationLevel.ALL).get(),
            ExecutionException.class, ResourceUnavailableException.class);
      }

      // complete futures from applyTransaction
      toBeCompleted.forEach(Runnable::run);
      // check replies
      for(CompletableFuture<RaftClientReply> f : writeFutures) {
        final RaftClientReply reply = getWithDefaultTimeout(f);
        Assert.assertTrue(reply.isSuccess());
      }
    }
  }
}
