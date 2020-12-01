/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ratis.grpc;

import org.apache.ratis.BaseTest;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.grpc.server.GrpcService;
import org.apache.ratis.metrics.JVMMetrics;
import org.apache.ratis.metrics.RatisMetricRegistry;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.impl.RaftServerTestUtil;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.TimeDuration;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.ratis.RaftTestUtil.waitForLeader;

public class TestGrpcMessageMetrics extends BaseTest
    implements MiniRaftClusterWithGrpc.FactoryGet {
  static {
    JVMMetrics.initJvmMetrics(TimeDuration.valueOf(10, TimeUnit.SECONDS));
  }

  public static final int NUM_SERVERS = 3;

  @Test
  public void testGrpcMessageMetrics() throws Exception {
    try(final MiniRaftCluster cluster = newCluster(NUM_SERVERS)) {
      cluster.start();
      sendMessages(cluster);
    }
  }

  static void sendMessages(MiniRaftCluster cluster) throws Exception {
    waitForLeader(cluster);
    try (final RaftClient client = cluster.createClient()) {
      CompletableFuture<RaftClientReply> replyFuture =
          client.async().send(new RaftTestUtil.SimpleMessage("abc"));
    }
    // Wait for commits to happen on leader
    JavaUtils.attempt(() -> assertMessageCount(cluster.getLeader()), 100, HUNDRED_MILLIS, cluster.getLeader().getId() + "-assertMessageCount", null);
  }

  static void assertMessageCount(RaftServer.Division server) {
    String serverId = server.getId().toString();
    GrpcService service = (GrpcService) RaftServerTestUtil.getServerRpc(server);
    RatisMetricRegistry registry = service.getServerInterceptor().getMetrics().getRegistry();
    String counter_prefix = serverId + "_" + "ratis.grpc.RaftServerProtocolService";
    Assert.assertTrue(registry.counter(counter_prefix + "_" + "requestVote" + "_OK_completed_total").getCount() > 0);
  }
}