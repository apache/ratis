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
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.retry.ClientRetryEvent;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.MiniRaftClusterWithGrpc;
import org.apache.ratis.protocol.exceptions.RaftRetryFailureException;
import org.apache.ratis.protocol.exceptions.TimeoutIOException;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.statemachine.SimpleStateMachine4Testing;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.TimeDuration;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.fail;

/**
 * Class to test {@link ExceptionDependentRetry}.
 */
public class TestExceptionDependentRetry extends BaseTest implements MiniRaftClusterWithGrpc.FactoryGet {

  @Test
  public void testExceptionDependentRetrySuccess() {
    ExceptionDependentRetry.Builder builder =
        ExceptionDependentRetry.newBuilder();

    int ioExceptionRetries = 1;
    int timeoutExceptionRetries = 2;
    int defaultExceptionRetries = 5;

    long ioExceptionSleepTime = 1;
    long timeoutExceptionSleepTime = 4;
    long defaultExceptionSleepTime = 10;
    int maxAttempts = 3;
    builder.setDefaultPolicy(RetryPolicies.retryUpToMaximumCountWithFixedSleep(defaultExceptionRetries,
        TimeDuration.valueOf(defaultExceptionSleepTime, TimeUnit.SECONDS)));
    builder.setExceptionToPolicy(IOException.class,
        RetryPolicies.retryUpToMaximumCountWithFixedSleep(ioExceptionRetries,
            TimeDuration.valueOf(ioExceptionSleepTime, TimeUnit.SECONDS)));
    builder.setExceptionToPolicy(TimeoutIOException.class,
        RetryPolicies.retryUpToMaximumCountWithFixedSleep(timeoutExceptionRetries,
            TimeDuration.valueOf(timeoutExceptionSleepTime, TimeUnit.SECONDS)));
    builder.setMaxAttempts(maxAttempts);


    ExceptionDependentRetry exceptionDependentRetry = builder.build();

    testException(ioExceptionRetries, maxAttempts,
        exceptionDependentRetry, new IOException(), ioExceptionSleepTime);
    testException(timeoutExceptionRetries, maxAttempts,
        exceptionDependentRetry, new TimeoutIOException("time out"),
        timeoutExceptionSleepTime);

    // now try with an exception which is not there in the map.
    testException(defaultExceptionRetries, maxAttempts,
        exceptionDependentRetry, new TimeoutException(),
        defaultExceptionSleepTime);

  }

  @Test
  public void testExceptionDependentRetryFailureWithExceptionDuplicate() {

    try {
      ExceptionDependentRetry.Builder builder =
          ExceptionDependentRetry.newBuilder();
      builder.setExceptionToPolicy(IOException.class,
          RetryPolicies.retryUpToMaximumCountWithFixedSleep(1,
              TimeDuration.valueOf(1, TimeUnit.SECONDS)));
      builder.setExceptionToPolicy(IOException.class,
          RetryPolicies.retryUpToMaximumCountWithFixedSleep(1,
              TimeDuration.valueOf(1, TimeUnit.SECONDS)));
      fail("testExceptionDependentRetryFailure failed");
    } catch (Exception ex) {
      Assert.assertEquals(IllegalStateException.class, ex.getClass());
    }

  }

  @Test
  public void testExceptionDependentRetryFailureWithExceptionMappedToNull() {
    try {
      ExceptionDependentRetry.Builder builder =
          ExceptionDependentRetry.newBuilder();
      builder.setExceptionToPolicy(IOException.class,
          RetryPolicies.retryUpToMaximumCountWithFixedSleep(1,
              TimeDuration.valueOf(1, TimeUnit.SECONDS)));
      builder.setExceptionToPolicy(IOException.class, null);
      fail("testExceptionDependentRetryFailure failed");
    } catch (Exception ex) {
      Assert.assertEquals(IllegalStateException.class, ex.getClass());
    }
  }

  @Test
  public void testExceptionDependentRetryFailureWithNoDefault() {

    try {
      ExceptionDependentRetry.Builder builder =
          ExceptionDependentRetry.newBuilder();
      builder.setExceptionToPolicy(IOException.class,
          RetryPolicies.retryUpToMaximumCountWithFixedSleep(1,
              TimeDuration.valueOf(1, TimeUnit.SECONDS)));
      builder.build();
      fail("testExceptionDependentRetryFailureWithNoDefault failed");
    } catch (Exception ex) {
      Assert.assertEquals(IllegalStateException.class, ex.getClass());
    }

    try {
      ExceptionDependentRetry.Builder builder =
          ExceptionDependentRetry.newBuilder();
      builder.setExceptionToPolicy(IOException.class,
          RetryPolicies.retryUpToMaximumCountWithFixedSleep(1,
              TimeDuration.valueOf(1, TimeUnit.SECONDS)));
      builder.setDefaultPolicy(null);
      fail("testExceptionDependentRetryFailureWithNoDefault failed");
    } catch (Exception ex) {
      Assert.assertEquals(IllegalStateException.class, ex.getClass());
    }
  }

  private void testException(int retries, int maxAttempts,
      ExceptionDependentRetry exceptionDependentRetry, Exception exception,
      long sleepTime) {
    for (int i = 0; i < retries + 1; i++) {
      RetryPolicy.Action action = exceptionDependentRetry
          .handleAttemptFailure(new ClientRetryEvent(i, null, exception));

      final boolean expected = i < retries && i < maxAttempts;
      Assert.assertEquals(expected, action.shouldRetry());
      if (expected) {
        Assert.assertEquals(sleepTime, action.getSleepTime().getDuration());
      } else {
        Assert.assertEquals(0L, action.getSleepTime().getDuration());
      }
    }
  }

  @Test
  public void testExceptionRetryAttempts() throws Exception {
    final RaftProperties prop = getProperties();
    RaftClientConfigKeys.Rpc.setRequestTimeout(prop, TimeDuration.valueOf(100, TimeUnit.MILLISECONDS));
    prop.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        SimpleStateMachine4Testing.class, StateMachine.class);
    RaftServerConfigKeys.Write.setElementLimit(prop, 1);

    runWithNewCluster(1, this::runTestExceptionRetryAttempts);
  }

  void runTestExceptionRetryAttempts(MiniRaftClusterWithGrpc cluster) throws Exception {
    final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster);
    final ExceptionDependentRetry policy = ExceptionDependentRetry.newBuilder()
        .setExceptionToPolicy(TimeoutIOException.class, MultipleLinearRandomRetry.parseCommaSeparated("1ms, 5"))
        .setDefaultPolicy(RetryPolicies.retryForeverNoSleep())
        .build();

    // create a client with the exception dependent policy
    try (final RaftClient client = cluster.createClient(policy)) {
      client.async().send(new RaftTestUtil.SimpleMessage("1")).get();
    }

    try (final RaftClient client = cluster.createClient(policy)) {
      SimpleStateMachine4Testing.get(leader).blockWriteStateMachineData();

      client.async().send(new RaftTestUtil.SimpleMessage("2")).get();
      Assert.fail("Test should have failed.");
    } catch (ExecutionException e) {
      RaftRetryFailureException rrfe = (RaftRetryFailureException) e.getCause();
      Assert.assertEquals(16, rrfe.getAttemptCount());
    } finally {
      SimpleStateMachine4Testing.get(leader).unblockWriteStateMachineData();
      cluster.shutdown();
    }
  }
}
