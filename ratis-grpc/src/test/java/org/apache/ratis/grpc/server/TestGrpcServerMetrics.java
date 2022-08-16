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
import static org.apache.ratis.grpc.metrics.GrpcServerMetrics.RATIS_GRPC_METRICS_LOG_APPENDER_PENDING_COUNT;
import static org.apache.ratis.grpc.metrics.GrpcServerMetrics.RATIS_GRPC_METRICS_LOG_APPENDER_SUCCESS;
import static org.apache.ratis.grpc.metrics.GrpcServerMetrics.RATIS_GRPC_METRICS_LOG_APPENDER_TIMEOUT;
import static org.apache.ratis.grpc.metrics.GrpcServerMetrics.RATIS_GRPC_METRICS_REQUESTS_TOTAL;
import static org.apache.ratis.grpc.metrics.GrpcServerMetrics.RATIS_GRPC_METRICS_REQUEST_RETRY_COUNT;
import static org.mockito.Mockito.when;

import java.util.SortedMap;
import java.util.function.Consumer;

import com.codahale.metrics.Gauge;
import org.apache.ratis.grpc.metrics.GrpcServerMetrics;
import org.apache.ratis.metrics.RatisMetricRegistry;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

public class TestGrpcServerMetrics {
  private static GrpcServerMetrics grpcServerMetrics;
  private static RatisMetricRegistry ratisMetricRegistry;
  private static RaftGroupId raftGroupId;
  private static RaftPeerId raftPeerId;
  private static RaftPeerId followerId;

  @BeforeClass
  public static void setUp() throws Exception {
    raftGroupId = RaftGroupId.randomId();
    raftPeerId = RaftPeerId.valueOf("TestId");
    followerId = RaftPeerId.valueOf("FollowerId");
    RaftGroupMemberId raftGroupMemberId = RaftGroupMemberId.valueOf(raftPeerId, raftGroupId);
    grpcServerMetrics = new GrpcServerMetrics(raftGroupMemberId.toString());
    ratisMetricRegistry = grpcServerMetrics.getRegistry();
  }

  @Test
  public void testGrpcLogAppenderLatencyTimer() throws Exception {
    for (boolean heartbeat : new boolean[] { true, false }) {
      RaftProtos.AppendEntriesRequestProto.Builder proto =
          RaftProtos.AppendEntriesRequestProto.newBuilder();
      if (!heartbeat) {
        proto.addEntries(RaftProtos.LogEntryProto.newBuilder().build());
      }
      GrpcLogAppender.AppendEntriesRequest req =
          new GrpcLogAppender.AppendEntriesRequest(proto.build(), followerId,
              grpcServerMetrics);
      Assert.assertEquals(0L, ratisMetricRegistry.timer(String.format(
          RATIS_GRPC_METRICS_LOG_APPENDER_LATENCY + GrpcServerMetrics
              .getHeartbeatSuffix(heartbeat), followerId.toString()))
          .getSnapshot().getMax());
      req.startRequestTimer();
      Thread.sleep(1000L);
      req.stopRequestTimer();
      Assert.assertTrue(ratisMetricRegistry.timer(String.format(
          RATIS_GRPC_METRICS_LOG_APPENDER_LATENCY + GrpcServerMetrics
              .getHeartbeatSuffix(heartbeat), followerId.toString()))
          .getSnapshot().getMax() > 1000L);
    }
  }

  @Test
  public void testGrpcLogRequestTotal() {
    for (boolean heartbeat : new boolean[] { true, false }) {
      long reqTotal = ratisMetricRegistry.counter(
          RATIS_GRPC_METRICS_REQUESTS_TOTAL + GrpcServerMetrics
              .getHeartbeatSuffix(heartbeat)).getCount();
      grpcServerMetrics.onRequestCreate(heartbeat);
      Assert.assertEquals(reqTotal + 1, ratisMetricRegistry.counter(
          RATIS_GRPC_METRICS_REQUESTS_TOTAL + GrpcServerMetrics
              .getHeartbeatSuffix(heartbeat)).getCount());
    }
  }

  @Test
  public void testGrpcLogRequestRetry() {
    Assert.assertEquals(0L, ratisMetricRegistry.counter(RATIS_GRPC_METRICS_REQUEST_RETRY_COUNT).getCount());
    grpcServerMetrics.onRequestRetry();
    Assert.assertEquals(1L, ratisMetricRegistry.counter(RATIS_GRPC_METRICS_REQUEST_RETRY_COUNT).getCount());
  }

  @Test
  public void testGrpcLogPendingRequestCount() {
    GrpcLogAppender.RequestMap pendingRequest = Mockito.mock(GrpcLogAppender.RequestMap.class);
    when(pendingRequest.logRequestsSize()).thenReturn(0);
    grpcServerMetrics.addPendingRequestsCount(raftPeerId.toString(),
        () -> pendingRequest.logRequestsSize());
    Assert.assertEquals(0, getGuageWithName(
            String.format(RATIS_GRPC_METRICS_LOG_APPENDER_PENDING_COUNT,
                raftPeerId.toString())).getValue());
    when(pendingRequest.logRequestsSize()).thenReturn(10);
    Assert.assertEquals(10, getGuageWithName(
            String.format(RATIS_GRPC_METRICS_LOG_APPENDER_PENDING_COUNT,
                raftPeerId.toString())).getValue());
  }

  private Gauge getGuageWithName(String gaugeName) {
    SortedMap<String, Gauge> gaugeMap =
        grpcServerMetrics.getRegistry().getGauges((s, metric) ->
            s.contains(gaugeName));
    return gaugeMap.get(gaugeMap.firstKey());
  }

  @Test
  public void testGrpcLogAppenderRequestCounters() {
    assertCounterIncremented(RATIS_GRPC_METRICS_LOG_APPENDER_NOT_LEADER, grpcServerMetrics::onRequestNotLeader);
    assertCounterIncremented(RATIS_GRPC_METRICS_LOG_APPENDER_INCONSISTENCY, grpcServerMetrics::onRequestInconsistency);

    for (boolean heartbeat : new boolean[] { true, false }) {
      assertCounterIncremented(RATIS_GRPC_METRICS_LOG_APPENDER_SUCCESS+ GrpcServerMetrics
              .getHeartbeatSuffix(heartbeat),
          follower -> grpcServerMetrics
              .onRequestSuccess(follower, heartbeat));
      assertCounterIncremented(RATIS_GRPC_METRICS_LOG_APPENDER_TIMEOUT+ GrpcServerMetrics
              .getHeartbeatSuffix(heartbeat),
          follower -> grpcServerMetrics.onRequestTimeout(follower, heartbeat));
    }
  }

  private void assertCounterIncremented(String counterVar, Consumer<String> incFunction) {
    String counter = String.format(counterVar, raftPeerId.toString());
    Assert.assertEquals(0L, ratisMetricRegistry.counter(counter).getCount());
    incFunction.accept(raftPeerId.toString());
    Assert.assertEquals(1L, ratisMetricRegistry.counter(counter).getCount());
  }
}
