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
package org.apache.ratis.retry;

import org.apache.ratis.BaseTest;
import org.apache.ratis.client.ClientRetryEvent;
import org.apache.ratis.client.retry.RequestTypeDependentRetryPolicy;
import org.apache.ratis.proto.RaftProtos.RaftClientRequestProto;
import org.apache.ratis.proto.RaftProtos.ReplicationLevel;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.util.TimeDuration;
import org.junit.Assert;
import org.junit.Test;

/** Test {@link RetryPolicy}. */
public class TestRetryPolicy extends BaseTest {
  @Override
  public int getGlobalTimeoutSeconds() {
    return 1;
  }

  @Test
  public void testRetryMultipleTimesWithFixedSleep() {
    final int n = 4;
    final TimeDuration sleepTime = HUNDRED_MILLIS;
    final RetryPolicy policy = RetryPolicies.retryUpToMaximumCountWithFixedSleep(n, sleepTime);
    for(int i = 1; i < 2*n; i++) {
      final int attempt = i;
      final RetryPolicy.Event event = () -> attempt;
      final RetryPolicy.Action action = policy.handleAttemptFailure(event);

      final boolean expected = i < n;
      Assert.assertEquals(expected, action.shouldRetry());
      if (expected) {
        Assert.assertEquals(sleepTime, action.getSleepTime());
      } else {
        Assert.assertEquals(0L, action.getSleepTime().getDuration());
      }
    }
  }

  @Test
  public void testRequestTypeDependentRetry() {
    final RequestTypeDependentRetryPolicy.Builder b = RequestTypeDependentRetryPolicy.newBuilder();
    final int n = 4;
    final TimeDuration writeSleep = HUNDRED_MILLIS;
    final RetryPolicies.RetryLimited writePolicy = RetryPolicies.retryUpToMaximumCountWithFixedSleep(n, writeSleep);
    b.set(RaftClientRequestProto.TypeCase.WRITE, writePolicy);
    b.set(RaftClientRequestProto.TypeCase.WATCH, RetryPolicies.noRetry());
    final RetryPolicy policy = b.build();
    LOG.info("policy = {}", policy);

    final RaftClientRequest staleReadRequest = newRaftClientRequest(RaftClientRequest.staleReadRequestType(1));
    final RaftClientRequest readRequest = newRaftClientRequest(RaftClientRequest.readRequestType());
    final RaftClientRequest writeRequest = newRaftClientRequest(RaftClientRequest.writeRequestType());
    final RaftClientRequest watchRequest = newRaftClientRequest(
        RaftClientRequest.watchRequestType(1, ReplicationLevel.MAJORITY));
    for(int i = 1; i < 2*n; i++) {
      { //write
        final ClientRetryEvent event = new ClientRetryEvent(i, writeRequest);
        final RetryPolicy.Action action = policy.handleAttemptFailure(event);

        final boolean expected = i < n;
        Assert.assertEquals(expected, action.shouldRetry());
        if (expected) {
          Assert.assertEquals(writeSleep, action.getSleepTime());
        } else {
          Assert.assertEquals(0L, action.getSleepTime().getDuration());
        }
      }

      { //read and stale read are using default
        final ClientRetryEvent event = new ClientRetryEvent(i, readRequest);
        final RetryPolicy.Action action = policy.handleAttemptFailure(event);
        Assert.assertTrue(action.shouldRetry());
        Assert.assertEquals(0L, action.getSleepTime().getDuration());
      }

      {
        final ClientRetryEvent event = new ClientRetryEvent(i, staleReadRequest);
        final RetryPolicy.Action action = policy.handleAttemptFailure(event);
        Assert.assertTrue(action.shouldRetry());
        Assert.assertEquals(0L, action.getSleepTime().getDuration());
      }

      { //watch has no retry
        final ClientRetryEvent event = new ClientRetryEvent(i, watchRequest);
        final RetryPolicy.Action action = policy.handleAttemptFailure(event);
        Assert.assertFalse(action.shouldRetry());
        Assert.assertEquals(0L, action.getSleepTime().getDuration());
      }
    }

  }

  private static RaftClientRequest newRaftClientRequest(RaftClientRequest.Type type) {
    return new RaftClientRequest(ClientId.randomId(), RaftPeerId.valueOf("s0"), RaftGroupId.randomId(), 1L, type);
  }
}