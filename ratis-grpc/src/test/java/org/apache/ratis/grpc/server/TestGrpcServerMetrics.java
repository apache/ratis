/**
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
package org.apache.ratis.grpc.server;

import static org.apache.ratis.grpc.metrics.GrpcServerMetrics.RATIS_GRPC_METRICS_LOG_APPENDER_INCONSISTENCY;
import static org.apache.ratis.grpc.metrics.GrpcServerMetrics.RATIS_GRPC_METRICS_LOG_APPENDER_LATENCY;
import static org.apache.ratis.grpc.metrics.GrpcServerMetrics.RATIS_GRPC_METRICS_LOG_APPENDER_NOT_LEADER;
import static org.apache.ratis.grpc.metrics.GrpcServerMetrics.RATIS_GRPC_METRICS_LOG_APPENDER_SUCCESS;
import static org.apache.ratis.grpc.metrics.GrpcServerMetrics.RATIS_GRPC_METRICS_LOG_APPENDER_TIMEOUT;
import static org.apache.ratis.grpc.metrics.GrpcServerMetrics.RATIS_GRPC_METRICS_REQUESTS_TOTAL;
import static org.apache.ratis.grpc.metrics.GrpcServerMetrics.RATIS_GRPC_METRICS_REQUEST_RETRY_COUNT;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.function.Consumer;

import org.apache.ratis.grpc.metrics.GrpcServerMetrics;
import org.apache.ratis.metrics.RatisMetricRegistry;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.impl.ServerState;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestGrpcServerMetrics {
  private static GrpcServerMetrics grpcServerMetrics;
  private static RatisMetricRegistry ratisMetricRegistry;
  private static RaftGroupId raftGroupId;
  private static RaftPeerId raftPeerId;

  @BeforeClass
  public static void setUp() throws Exception {
    RaftServerImpl raftServer = mock(RaftServerImpl.class);
    ServerState serverStateMock = mock(ServerState.class);
    when(raftServer.getState()).thenReturn(serverStateMock);
    when(serverStateMock.getLastLeaderElapsedTimeMs()).thenReturn(1000L);
    raftGroupId = RaftGroupId.randomId();
    raftPeerId = RaftPeerId.valueOf("TestId");
    RaftGroupMemberId raftGroupMemberId = RaftGroupMemberId.valueOf(raftPeerId, raftGroupId);
    when(raftServer.getMemberId()).thenReturn(raftGroupMemberId);
    grpcServerMetrics = new GrpcServerMetrics(raftGroupMemberId.toString());
    ratisMetricRegistry = grpcServerMetrics.getRegistry();
  }

  @Test
  public void testGrpcLogAppenderLatencyTimer() throws Exception {
    RaftProtos.AppendEntriesRequestProto.Builder proto = RaftProtos.AppendEntriesRequestProto.newBuilder();
    GrpcLogAppender.AppendEntriesRequest req =
        new GrpcLogAppender.AppendEntriesRequest(proto.build(),
            grpcServerMetrics.getGrpcLogAppenderLatencyTimer(raftPeerId.toString()));
    Assert.assertEquals(0L, ratisMetricRegistry.timer(String.format(
        RATIS_GRPC_METRICS_LOG_APPENDER_LATENCY, raftPeerId.toString())).getSnapshot().getMax());
    req.startRequestTimer();
    Thread.sleep(1000L);
    req.stopRequestTimer();
    Assert.assertTrue(ratisMetricRegistry.timer(String.format(
        RATIS_GRPC_METRICS_LOG_APPENDER_LATENCY, raftPeerId.toString())).getSnapshot().getMax() > 1000L);
  }

  @Test
  public void testGrpcLogRequestTotal() {
    Assert.assertEquals(0L, ratisMetricRegistry.counter(RATIS_GRPC_METRICS_REQUESTS_TOTAL).getCount());
    grpcServerMetrics.onRequestCreate();
    Assert.assertEquals(1L, ratisMetricRegistry.counter(RATIS_GRPC_METRICS_REQUESTS_TOTAL).getCount());
  }

  @Test
  public void testGrpcLogRequestRetry() {
    Assert.assertEquals(0L, ratisMetricRegistry.counter(RATIS_GRPC_METRICS_REQUEST_RETRY_COUNT).getCount());
    grpcServerMetrics.onRequestRetry();
    Assert.assertEquals(1L, ratisMetricRegistry.counter(RATIS_GRPC_METRICS_REQUEST_RETRY_COUNT).getCount());
  }

  @Test
  public void testGrpcLogAppenderRequestCounters() {
    assertCounterIncremented(RATIS_GRPC_METRICS_LOG_APPENDER_SUCCESS, grpcServerMetrics::onRequestSuccess);
    assertCounterIncremented(RATIS_GRPC_METRICS_LOG_APPENDER_NOT_LEADER, grpcServerMetrics::onRequestNotLeader);
    assertCounterIncremented(RATIS_GRPC_METRICS_LOG_APPENDER_INCONSISTENCY, grpcServerMetrics::onRequestInconsistency);
    assertCounterIncremented(RATIS_GRPC_METRICS_LOG_APPENDER_TIMEOUT, grpcServerMetrics::onRequestTimeout);
  }

  private void assertCounterIncremented(String counterVar, Consumer<String> incFunction) {
    String counter = String.format(counterVar, raftPeerId.toString());
    Assert.assertEquals(0L, ratisMetricRegistry.counter(counter).getCount());
    incFunction.accept(raftPeerId.toString());
    Assert.assertEquals(1L, ratisMetricRegistry.counter(counter).getCount());
  }
}
