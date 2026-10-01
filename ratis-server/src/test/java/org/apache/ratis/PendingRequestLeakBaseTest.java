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

import org.apache.ratis.RaftTestUtil.SimpleMessage;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.SimpleStateMachine4Testing;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * A request rejected by the state machine in preAppend must give its pending-request
 * resources back.  Otherwise the write capacity of the leader shrinks on every rejected
 * request until the leader answers every write with a ResourceUnavailableException.
 */
public abstract class PendingRequestLeakBaseTest<CLUSTER extends MiniRaftCluster>
    extends BaseTest
    implements MiniRaftCluster.Factory.Get<CLUSTER> {

  private static volatile boolean failPreAppend = false;

  /**
   * Rejects every transaction in preAppend while {@link #failPreAppend} is set.
   * The exception is built with leaderShouldStepDown = false so that the leader keeps its
   * {@link org.apache.ratis.server.impl.LeaderStateImpl}; a step down would discard the pending
   * request resources altogether and hide the leak.
   */
  public static class RejectingStateMachine extends SimpleStateMachine4Testing {
    @Override
    public TransactionContext preAppendTransaction(TransactionContext trx) throws IOException {
      if (failPreAppend) {
        throw new StateMachineException("Rejected by the state machine", false);
      }
      return trx;
    }
  }

  /** Small enough that a leak is visible after a handful of rejected requests. */
  private static final int WRITE_ELEMENT_LIMIT = 4;

  {
    final RaftProperties p = setStateMachine(RejectingStateMachine.class);
    RaftServerConfigKeys.Write.setElementLimit(p, WRITE_ELEMENT_LIMIT);
  }

  @Test
  public void testRejectedRequestsDoNotLeakPendingRequestResources() throws Exception {
    runWithNewCluster(1, this::runTest);
  }

  private void runTest(CLUSTER cluster) throws Exception {
    final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster);

    try (RaftClient client = cluster.createClient(leader.getId(), RetryPolicies.noRetry())) {
      // Reject more requests than the element limit.  Each one is answered, so nothing is
      // still pending afterwards, and the whole write capacity must be available again.
      failPreAppend = true;
      try {
        for (int i = 0; i < WRITE_ELEMENT_LIMIT * 2; i++) {
          final int index = i;
          testFailureCase("rejected write " + index,
              () -> client.io().send(new SimpleMessage("rejected" + index)),
              StateMachineException.class);
        }
      } finally {
        failPreAppend = false;
      }

      // Without releasing the permits of the rejected requests, this would fail with
      // ResourceUnavailableException even though no request is actually pending.
      // With releasing the permits of the rejected requests, the future request would pass.
      final RaftClientReply reply = client.io().send(new SimpleMessage("accepted"));
      Assertions.assertTrue(reply.isSuccess(),
          () -> "Leader rejected a write after " + (WRITE_ELEMENT_LIMIT * 2)
              + " requests were refused by the state machine: " + reply);
    }
  }
}
