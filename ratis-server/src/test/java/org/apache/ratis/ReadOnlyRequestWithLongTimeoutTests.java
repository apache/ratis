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
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.ReadException;
import org.apache.ratis.protocol.exceptions.TimeoutIOException;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.Log4jUtils;
import org.apache.ratis.util.TimeDuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public abstract class ReadOnlyRequestWithLongTimeoutTests<CLUSTER extends MiniRaftCluster>
  extends BaseTest
  implements MiniRaftCluster.Factory.Get<CLUSTER> {

  {
    Log4jUtils.setLogLevel(RaftServer.Division.LOG, Level.DEBUG);
  }

  static final int NUM_SERVERS = 3;

  static final String INCREMENT = "INCREMENT";
  static final String WAIT_AND_INCREMENT = "WAIT_AND_INCREMENT";
  static final String TIMEOUT_INCREMENT = "TIMEOUT_INCREMENT";
  static final String QUERY = "QUERY";
  final Message incrementMessage = new RaftTestUtil.SimpleMessage(INCREMENT);
  final Message waitAndIncrementMessage = new RaftTestUtil.SimpleMessage(WAIT_AND_INCREMENT);
  final Message timeoutMessage = new RaftTestUtil.SimpleMessage(TIMEOUT_INCREMENT);
  final Message queryMessage = new RaftTestUtil.SimpleMessage(QUERY);

  @Before
  public void setup() {
    final RaftProperties p = getProperties();
    p.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        ReadOnlyRequestTests.CounterStateMachine.class, StateMachine.class);

    RaftServerConfigKeys.Read.setOption(p, RaftServerConfigKeys.Read.Option.LINEARIZABLE);
    RaftServerConfigKeys.Read.setTimeout(p, TimeDuration.ONE_SECOND);
    RaftServerConfigKeys.Rpc.setFirstElectionTimeoutMin(p, TimeDuration.valueOf(150, TimeUnit.MILLISECONDS));
    RaftServerConfigKeys.Rpc.setFirstElectionTimeoutMax(p, TimeDuration.valueOf(300, TimeUnit.MILLISECONDS));
    RaftServerConfigKeys.Rpc.setTimeoutMin(p, TimeDuration.valueOf(3, TimeUnit.SECONDS));
    RaftServerConfigKeys.Rpc.setTimeoutMax(p, TimeDuration.valueOf(6, TimeUnit.SECONDS));
    RaftServerConfigKeys.Rpc.setRequestTimeout(p, TimeDuration.valueOf(10, TimeUnit.SECONDS));
  }

  @Test
  public void testLinearizableReadParallel() throws Exception {
    runWithNewCluster(NUM_SERVERS, this::testLinearizableReadParallelImpl);
  }

  private void testLinearizableReadParallelImpl(CLUSTER cluster) throws Exception {
    RaftTestUtil.waitForLeader(cluster);
    final RaftPeerId leaderId = cluster.getLeader().getId();

    try (RaftClient client = cluster.createClient(leaderId, RetryPolicies.noRetry())) {
      final RaftClientReply reply = client.io().send(incrementMessage);
      Assert.assertTrue(reply.isSuccess());

      client.async().send(waitAndIncrementMessage);
      Thread.sleep(100);

      RaftClientReply staleValueBefore = client.io().sendStaleRead(queryMessage, 0, leaderId);

      Assert.assertEquals(1, ReadOnlyRequestTests.retrieve(staleValueBefore));

      RaftClientReply linearizableReadValue = client.io().sendReadOnly(queryMessage);
      Assert.assertEquals(2, ReadOnlyRequestTests.retrieve(linearizableReadValue));
    }
  }

  @Test
  public void testLinearizableReadTimeout() throws Exception {
    runWithNewCluster(NUM_SERVERS, this::testLinearizableReadTimeoutImpl);
  }

  private void testLinearizableReadTimeoutImpl(CLUSTER cluster) throws Exception {
    RaftTestUtil.waitForLeader(cluster);
    final RaftPeerId leaderId = cluster.getLeader().getId();

    try (RaftClient client = cluster.createClient(leaderId, RetryPolicies.noRetry())) {
      final RaftClientReply reply = client.io().send(incrementMessage);
      Assert.assertTrue(reply.isSuccess());

      final CompletableFuture<RaftClientReply> asyncTimeoutReply = client.async().send(timeoutMessage);
      Thread.sleep(100);

      Assert.assertThrows(ReadException.class, () -> {
        final RaftClientReply timeoutReply = client.io().sendReadOnly(queryMessage);
        Assert.assertTrue(timeoutReply.getException().getCause() instanceof TimeoutIOException);
      });

      asyncTimeoutReply.join();
    }
  }
}
