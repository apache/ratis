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

import org.apache.ratis.BaseTest;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftClientReply;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.apache.ratis.RaftTestUtil.waitForLeader;

/**
 * Verify a cluster comes up and processes requests when the worker
 * event-loop thread count is capped via
 * {@link GrpcConfigKeys.Server#WORKER_EVENT_LOOP_THREADS_KEY} and
 * {@link GrpcConfigKeys.Client#WORKER_EVENT_LOOP_THREADS_KEY}.
 *
 * <p>Regression: RATIS-2529 — gRPC worker threads permanently inflate to
 * {@code availableProcessors * 2} after follower restart catch-up.
 */
public class TestGrpcWorkerEventLoopThreads extends BaseTest {

  @Test
  public void testClusterWithCappedWorkerEventLoopThreads() throws Exception {
    final String[] ids = {"s0", "s1", "s2"};
    final RaftProperties properties = new RaftProperties();
    GrpcConfigKeys.Server.setWorkerEventLoopThreads(properties, 2);
    GrpcConfigKeys.Client.setWorkerEventLoopThreads(properties, 1);

    try (MiniRaftClusterWithGrpc cluster = new MiniRaftClusterWithGrpc(ids, properties, null)) {
      cluster.start();
      waitForLeader(cluster);
      try (RaftClient client = cluster.createClient()) {
        final RaftClientReply reply = client.io().send(new RaftTestUtil.SimpleMessage("hello"));
        Assertions.assertTrue(reply.isSuccess());
      }
    }
  }
}
