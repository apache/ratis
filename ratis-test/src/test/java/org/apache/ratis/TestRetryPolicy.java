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

import org.apache.ratis.proto.RaftProtos.ReplicationLevel;
import org.apache.ratis.proto.RaftProtos.RaftClientRequestProto;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.util.TimeDuration;
import org.junit.Assert;
import org.junit.Test;


public class TestRetryPolicy extends BaseTest {

  @Test
  public void testRetryMultipleTimesWithFixedSleep() {
    final int n = 4;
    final TimeDuration sleepTime = HUNDRED_MILLIS;
    final RetryPolicy policy = RetryPolicies.retryUpToMaximumCountWithFixedSleep(n, sleepTime);
    final RaftClientRequest request = newRaftClientRequest(RaftClientRequest.readRequestType());
    for(int i = 1; i < 2*n; i++) {
      final boolean expected = i < n;
      Assert.assertEquals(expected, policy.shouldRetry(i, request));
      if (expected) {
        Assert.assertEquals(sleepTime, policy.getSleepTime(i, request));
      } else {
        Assert.assertEquals(0L, policy.getSleepTime(i, request).getDuration());
      }
    }
  }

  @Test
  public void testRequestTypeDependentRetry() {
    final RetryPolicies.RequestTypeDependentRetry.Builder b = RetryPolicies.RequestTypeDependentRetry.newBuilder();
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
        final boolean expected = i < n;
        Assert.assertEquals(expected, policy.shouldRetry(i, writeRequest));
        if (expected) {
          Assert.assertEquals(writeSleep, policy.getSleepTime(i, writeRequest));
        } else {
          Assert.assertEquals(0L, policy.getSleepTime(i, writeRequest).getDuration());
        }
      }

      { //read and stale read are using default
        Assert.assertTrue(policy.shouldRetry(i, readRequest));
        Assert.assertEquals(0L, policy.getSleepTime(i, readRequest).getDuration());

        Assert.assertTrue(policy.shouldRetry(i, staleReadRequest));
        Assert.assertEquals(0L, policy.getSleepTime(i, staleReadRequest).getDuration());
      }

      { //watch has no retry
        Assert.assertFalse(policy.shouldRetry(i, watchRequest));
        Assert.assertEquals(0L, policy.getSleepTime(i, watchRequest).getDuration());
      }
    }

  }

  private static RaftClientRequest newRaftClientRequest(RaftClientRequest.Type type) {
    return new RaftClientRequest(ClientId.randomId(), RaftPeerId.valueOf("s0"), RaftGroupId.randomId(), 1L, type);
  }
}