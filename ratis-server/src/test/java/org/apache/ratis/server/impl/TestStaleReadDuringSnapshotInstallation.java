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
package org.apache.ratis.server.impl;

import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.exceptions.ReadException;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.simulation.MiniRaftClusterWithSimulatedRpc;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachine4Testing;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;

/**
 * Test stale reads being rejected while a snapshot installation is in progress: the follower
 * serving the stale read is exactly the node whose state machine is paused/reinitialized by the
 * installation, so the read must be rejected with a retriable error instead of querying a paused
 * state machine (see {@link ReadRequestHandler#staleReadAsync}).
 */
public class TestStaleReadDuringSnapshotInstallation {
  private static final int NUM_SERVERS = 3;

  private final MiniRaftClusterWithSimulatedRpc cluster = MiniRaftClusterWithSimulatedRpc
      .FACTORY.newCluster(NUM_SERVERS, getProperties());

  private static RaftProperties getProperties() {
    final RaftProperties properties = new RaftProperties();
    properties.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        SimpleStateMachine4Testing.class, StateMachine.class);
    return properties;
  }

  @BeforeEach
  public void setup() throws IOException {
    cluster.start();
  }

  @AfterEach
  public void tearDown() {
    cluster.shutdown();
  }

  @Test
  @Timeout(120)
  public void testStaleReadRejectedWhileSnapshotInstallationInProgress() throws Exception {
    RaftTestUtil.waitForLeader(cluster);

    try (final RaftClient client = cluster.createClient()) {
      for (int i = 0; i < 5; i++) {
        Assertions.assertTrue(client.io().send(Message.valueOf("m" + i)).isSuccess());
      }

      final RaftServer.Division follower = cluster.getFollowers().get(0);
      final SnapshotInstallationHandler handler =
          ((RaftServerImpl) follower).getSnapshotInstallationHandler();
      final Message message = Message.valueOf("m4");

      // send() only guarantees that the leader has committed; the follower applies asynchronously.
      // Wait until the follower has applied m4, otherwise a stale read may hit the state machine
      // before the queried entry is applied (see SimpleStateMachine4Testing#query).
      final long m4Index = cluster.getLeader().getRaftLog().getLastCommittedIndex();
      for (int i = 0; follower.getInfo().getLastAppliedIndex() < m4Index; i++) {
        Assertions.assertTrue(i < 100, () -> follower.getId() + ": fails to catch up, lastAppliedIndex="
            + follower.getInfo().getLastAppliedIndex() + " < " + m4Index);
        Thread.sleep(100);
      }

      // Regression guard: a stale read on a healthy follower still succeeds.
      final RaftClientReply before = client.io().sendStaleRead(message, m4Index, follower.getId());
      Assertions.assertTrue(before.isSuccess());

      // Simulate a snapshot installation in progress on the follower: the gate must reject
      // the stale read with a retriable ReadException instead of querying the paused state
      // machine. Use a no-retry client so the exception surfaces immediately (the default
      // test policy would keep retrying it -- see BlockingImpl#sendRequestWithRetry).
      handler.setInProgressInstallSnapshotIndex(1);
      try (final RaftClient noRetryClient = cluster.createClient(follower.getId(), RetryPolicies.noRetry())) {
        final IOException e = Assertions.assertThrows(IOException.class,
            () -> noRetryClient.io().sendStaleRead(message, m4Index, follower.getId()));
        Assertions.assertTrue(e instanceof ReadException, () -> "Unexpected exception: " + e);
        Assertions.assertTrue(e.getMessage().contains("stale read"), () -> e.getMessage());
        Assertions.assertTrue(e.getMessage().contains("in progress"), () -> e.getMessage());
      } finally {
        handler.setInProgressInstallSnapshotIndex(RaftLog.INVALID_LOG_INDEX);
      }

      // After the (simulated) installation completed: stale reads work again.
      final RaftClientReply after = client.io().sendStaleRead(message, m4Index, follower.getId());
      Assertions.assertTrue(after.isSuccess());
    }
  }
}
