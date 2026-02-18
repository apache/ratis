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

package org.apache.ratis.netty.client;

import org.apache.ratis.BaseTest;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.DataStreamClient;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.datastream.DataStreamTestUtils.MultiDataStreamStateMachine;
import org.apache.ratis.datastream.MiniRaftClusterWithRpcTypeGrpcAndDataStreamTypeNetty;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.retry.ExponentialBackoffRetry;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.util.IOUtils;
import org.apache.ratis.util.TimeDuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 120)
public class TestNettyDataStreamReconnectWithGrpcCluster extends BaseTest
    implements MiniRaftClusterWithRpcTypeGrpcAndDataStreamTypeNetty.FactoryGet {
  {
    setStateMachine(MultiDataStreamStateMachine.class);
  }

  @Test
  public void testReconnectConfigApplied() throws Exception {
    final RaftProperties properties = getProperties();
    final TimeDuration reconnectDelay = TimeDuration.valueOf(200, TimeUnit.MILLISECONDS);
    final TimeDuration reconnectMaxDelay = TimeDuration.valueOf(400, TimeUnit.MILLISECONDS);
    NettyConfigKeys.DataStream.Client.setReconnectPolicy(properties,
        "ExponentialBackoffRetry," + reconnectDelay + "," + reconnectMaxDelay + ",10");

    runWithNewCluster(1, cluster -> {
      RaftTestUtil.waitForLeader(cluster);
      final RaftPeer primary = cluster.getLeader().getPeer();

      final RaftClient client = cluster.createClient(primary);
      try {
        final DataStreamClient dataStreamClient = (DataStreamClient) client.getDataStreamApi();
        final NettyClientStreamRpc rpc = (NettyClientStreamRpc) dataStreamClient.getClientRpc();

        // Verify reconnect configuration is applied.
        final RetryPolicy policy = rpc.getReconnectPolicy();
        assertTrue(policy instanceof ExponentialBackoffRetry);
        assertSleepInRange(policy, 0, reconnectDelay, reconnectMaxDelay);
        assertSleepInRange(policy, 1, reconnectDelay, reconnectMaxDelay);

        // Verify the data stream channel can be established.
        assertTrue(rpc.waitForChannelActive(TimeDuration.valueOf(5, TimeUnit.SECONDS)),
            "Data stream channel should be active");
      } finally {
        IOUtils.cleanup(LOG, client);
      }
    });
  }

  private static void assertSleepInRange(RetryPolicy policy, int attempt, TimeDuration base, TimeDuration max) {
    final RetryPolicy.Action action = policy.handleAttemptFailure(() -> attempt);
    assertTrue(action.shouldRetry());

    final long baseMillis = base.toLong(TimeUnit.MILLISECONDS);
    final long maxMillis = max.toLong(TimeUnit.MILLISECONDS);
    final long expected = Math.min(maxMillis, baseMillis * (1L << attempt));
    final long actual = action.getSleepTime().toLong(TimeUnit.MILLISECONDS);

    assertTrue(actual >= expected / 2, "delay too small: " + actual);
    assertTrue(actual <= expected + expected / 2, "delay too large: " + actual);
  }

}
