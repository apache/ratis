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

import org.apache.ratis.LogAppenderTests;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.metrics.GrpcServerMetrics;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.leader.FollowerInfo;
import org.apache.ratis.server.leader.LogAppender;
import org.apache.ratis.server.impl.RaftServerTestUtil;
import org.apache.ratis.statemachine.impl.SimpleStateMachine4Testing;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Slf4jUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.event.Level;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.ratis.server.impl.BlockRequestHandlingInjection;
import org.apache.ratis.util.CodeInjectionForTesting;

import static org.apache.ratis.RaftTestUtil.waitForLeader;
import static org.apache.ratis.grpc.server.GrpcServerProtocolService.GRPC_SERVER_HANDLE_ERROR;

public class TestLogAppenderWithGrpc
    extends LogAppenderTests<MiniRaftClusterWithGrpc>
    implements MiniRaftClusterWithGrpc.FactoryGet {
  {
    Slf4jUtils.setLogLevel(FollowerInfo.LOG, Level.DEBUG);
  }

  public static Collection<Boolean[]> data() {
    return Arrays.asList((new Boolean[][] {{Boolean.FALSE}, {Boolean.TRUE}}));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testPendingLimits(Boolean separateHeartbeat) throws IOException, InterruptedException {
    GrpcConfigKeys.Server.setHeartbeatChannel(getProperties(), separateHeartbeat);
    int maxAppends = 10;
    RaftProperties properties = new RaftProperties();
    properties.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        SimpleStateMachine4Testing.class, StateMachine.class);
    GrpcConfigKeys.Server.setLeaderOutstandingAppendsMax(properties, maxAppends);
    RaftServerConfigKeys.Log.Appender.setBufferElementLimit(properties, 1);
    MiniRaftClusterWithGrpc cluster = getFactory().newCluster(3, properties);
    cluster.start();

    // client and leader setup
    try (final RaftClient client = cluster.createClient(cluster.getGroup())) {
      final RaftServer.Division leader = waitForLeader(cluster);
      RaftClientReply reply = client.io().send(new RaftTestUtil.SimpleMessage("m"));
      client.io().watch(reply.getLogIndex(), RaftProtos.ReplicationLevel.ALL_COMMITTED);
      long initialNextIndex = RaftServerTestUtil.getNextIndex(leader);

      for (RaftServer.Division server : cluster.getFollowers()) {
        // block the appends in the follower
        SimpleStateMachine4Testing.get(server).blockWriteStateMachineData();
      }
      Collection<CompletableFuture<RaftClientReply>> futures = new ArrayList<>(maxAppends * 2);
      for (int i = 0; i < maxAppends * 2; i++) {
        futures.add(client.async().send(new RaftTestUtil.SimpleMessage("m")));
      }

      JavaUtils.attempt(() -> {
        for (long nextIndex : leader.getInfo().getFollowerNextIndices()) {
          // Verify nextIndex does not progress due to pendingRequests limit
          Assertions.assertEquals(initialNextIndex + maxAppends, nextIndex);
        }
      }, 10, ONE_SECOND, "matching nextIndex", LOG);
      for (RaftServer.Division server : cluster.getFollowers()) {
        // unblock the appends in the follower
        SimpleStateMachine4Testing.get(server).unblockWriteStateMachineData();
      }

      JavaUtils.allOf(futures).join();
      cluster.shutdown();
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testRestartLogAppender(Boolean separateHeartbeat) throws Exception {
    GrpcConfigKeys.Server.setHeartbeatChannel(getProperties(), separateHeartbeat);
    runWithNewCluster(2, this::runTestRestartLogAppender);
  }

  private void runTestRestartLogAppender(MiniRaftClusterWithGrpc cluster) throws Exception {
    final RaftServer.Division leader = waitForLeader(cluster);

    int messageCount = 0;
    // Send some messages
    try(RaftClient client = cluster.createClient(leader.getId())) {
      for(int i = 0; i < 10; i++) {
        final RaftClientReply reply = client.io().send(new RaftTestUtil.SimpleMessage("m" + ++messageCount));
        Assertions.assertTrue(reply.isSuccess());
      }
    }

    // assert INCONSISTENCY counter == 0
    final GrpcServerMetrics leaderMetrics = new GrpcServerMetrics(leader.getMemberId().toString());
    final String counter = String.format(GrpcServerMetrics.RATIS_GRPC_METRICS_LOG_APPENDER_INCONSISTENCY,
        cluster.getFollowers().iterator().next().getMemberId().getPeerId());
    Assertions.assertEquals(0L, leaderMetrics.getRegistry().counter(counter).getCount());

    // restart LogAppender
    RaftServerTestUtil.restartLogAppenders(leader);

    // Send some more messages
    try(RaftClient client = cluster.createClient(leader.getId())) {
      for(int i = 0; i < 10; i++) {
        final RaftClientReply reply = client.io().send(new RaftTestUtil.SimpleMessage("m" + ++messageCount));
        Assertions.assertTrue(reply.isSuccess());
      }
    }

    final RaftServer.Division newLeader = waitForLeader(cluster);
    if (leader == newLeader) {
      final GrpcServerMetrics newleaderMetrics = new GrpcServerMetrics(leader.getMemberId().toString());

      // assert INCONSISTENCY counter >= 1
      // If old LogAppender die before new LogAppender start, INCONSISTENCY equal to 1,
      // else INCONSISTENCY greater than 1
      Assertions.assertTrue(newleaderMetrics.getRegistry().counter(counter).getCount() >= 1L);
    }
  }

  /**
   * Verify that old LogAppender instances are properly cleaned up (gRPC streams terminated)
   * after restartLogAppenders. Without the fix, gRPC holds references to unterminated
   * stream response handlers, preventing old LogAppender instances from being collected.
   */
  @ParameterizedTest
  @MethodSource("data")
  public void testLogAppenderStreamCleanupOnRestart(Boolean separateHeartbeat) throws Exception {
    GrpcConfigKeys.Server.setHeartbeatChannel(getProperties(), separateHeartbeat);
    runWithNewCluster(3, this::runTestLogAppenderStreamCleanupOnRestart);
  }

  private void runTestLogAppenderStreamCleanupOnRestart(MiniRaftClusterWithGrpc cluster) throws Exception {
    final RaftServer.Division leader = waitForLeader(cluster);
    final RaftPeerId leaderId = leader.getId();

    try (RaftClient client = cluster.createClient(leaderId)) {
      for (int i = 0; i < 10; i++) {
        final RaftClientReply reply = client.io().send(new RaftTestUtil.SimpleMessage("m" + i));
        Assertions.assertTrue(reply.isSuccess());
      }
    }

    final List<WeakReference<LogAppender>> weakRefs =
        RaftServerTestUtil.getLogAppenders(leader)
            .map(WeakReference::new)
            .collect(Collectors.toList());
    Assertions.assertFalse(weakRefs.isEmpty());

    RaftServerTestUtil.restartLogAppenders(leader);

    try (RaftClient client = cluster.createClient(leaderId)) {
      for (int i = 0; i < 10; i++) {
        client.io().send(new RaftTestUtil.SimpleMessage("after-" + i));
      }
    }

    // Old appenders should be GC-able once their gRPC streams are terminated.
    // If streams leaked, gRPC retains references to the response handlers
    // (inner classes of GrpcLogAppender), preventing collection.
    JavaUtils.attempt(() -> {
      System.gc();
      for (WeakReference<LogAppender> ref : weakRefs) {
        Assertions.assertNull(ref.get(),
            "Old LogAppender should be garbage collected after stream cleanup");
      }
    }, 20, ONE_SECOND, "old-appender-gc", LOG);
  }

  /**
   * Verify that the follower's ServerRequestStreamObserver cleans up previousOnNext
   * when handleError is triggered. This injects failures at the APPEND_ENTRIES point
   * on a specific follower, causing process(request) to fail and handleError to be called.
   * Without the fix, previousOnNext retains the last PendingServerRequest (including the
   * full AppendEntriesRequestProto with log entry data) after handleError closes the stream.
   */
  @ParameterizedTest
  @MethodSource("data")
  public void testFollowerHandleErrorCleanup(Boolean separateHeartbeat) throws Exception {
    GrpcConfigKeys.Server.setHeartbeatChannel(getProperties(), separateHeartbeat);
    runWithNewCluster(3, this::runTestFollowerHandleErrorCleanup);
  }

  private void runTestFollowerHandleErrorCleanup(MiniRaftClusterWithGrpc cluster) throws Exception {
    final RaftServer.Division leader = waitForLeader(cluster);
    final RaftPeerId leaderId = leader.getId();

    try (RaftClient client = cluster.createClient(leaderId)) {
      for (int i = 0; i < 5; i++) {
        Assertions.assertTrue(client.io().send(
            new RaftTestUtil.SimpleMessage("init-" + i)).isSuccess());
      }
    }

    final RaftPeerId followerId = cluster.getFollowers().get(0).getId();
    final String APPEND_ENTRIES = "RaftServerImpl.appendEntries";
    final AtomicBoolean shouldFail = new AtomicBoolean(false);
    final AtomicInteger handleErrorCount = new AtomicInteger(0);
    final AtomicInteger leakCount = new AtomicInteger(0);

    CodeInjectionForTesting.put(APPEND_ENTRIES, (localId, remoteId, args) -> {
      if (shouldFail.get() && localId.toString().equals(followerId.toString())) {
        throw new RuntimeException("Injected failure for handleError test");
      }
      return false;
    });
    CodeInjectionForTesting.put(GRPC_SERVER_HANDLE_ERROR, (localId, remoteId, args) -> {
      handleErrorCount.incrementAndGet();
      if (args != null && args.length > 0 && args[0] != null) {
        leakCount.incrementAndGet();
      }
      return false;
    });

    try {
      final int numCycles = 3;
      for (int cycle = 0; cycle < numCycles; cycle++) {
        LOG.info("=== HandleError cycle {} ===", cycle);

        shouldFail.set(true);

        try (RaftClient client = cluster.createClient(leaderId)) {
          for (int i = 0; i < 5; i++) {
            client.io().send(new RaftTestUtil.SimpleMessage("fail-" + cycle + "-" + i));
          }
        }

        JavaUtils.attempt(() -> {
          final long leaderCommit = leader.getRaftLog().getLastCommittedIndex();
          Assertions.assertTrue(leaderCommit > 0);
        }, 10, ONE_SECOND, "leader-commit-cycle-" + cycle, LOG);

        shouldFail.set(false);

        try (RaftClient client = cluster.createClient(leaderId)) {
          for (int i = 0; i < 5; i++) {
            Assertions.assertTrue(client.io().send(
                new RaftTestUtil.SimpleMessage("recover-" + cycle + "-" + i)).isSuccess());
          }
        }

        final int c = cycle;
        JavaUtils.attempt(() -> {
          final RaftServer.Division f = cluster.getDivision(followerId);
          Assertions.assertTrue(f.getInfo().getLastAppliedIndex() > 0,
              "Follower " + followerId + " should recover after handleError");
        }, 10, ONE_SECOND, "follower-recover-" + c, LOG);
      }

      try (RaftClient client = cluster.createClient(leaderId)) {
        final RaftClientReply reply = client.io().send(new RaftTestUtil.SimpleMessage("final"));
        Assertions.assertTrue(reply.isSuccess());
        client.io().watch(reply.getLogIndex(), RaftProtos.ReplicationLevel.ALL_COMMITTED);
      }

      Assertions.assertTrue(handleErrorCount.get() > 0,
          "handleError should have been triggered by the injected failures");
      Assertions.assertEquals(0, leakCount.get(),
          "previousOnNext should be cleaned up in handleError to prevent memory leaks");
    } finally {
      CodeInjectionForTesting.put(APPEND_ENTRIES, BlockRequestHandlingInjection.getInstance());
      CodeInjectionForTesting.remove(GRPC_SERVER_HANDLE_ERROR);
    }
  }

  /**
   * Reproduce the StreamObserver leak by repeatedly killing and restarting a follower.
   * Each kill triggers onError on the leader's response handler, calling
   * resetClient -> stop(ERROR) -> cancelStream. Without the fix, the old gRPC streams
   * are never terminated and accumulate on both leader and follower sides.
   */
  @ParameterizedTest
  @MethodSource("data")
  public void testStreamObserverCleanupOnFollowerKillRestart(Boolean separateHeartbeat) throws Exception {
    GrpcConfigKeys.Server.setHeartbeatChannel(getProperties(), separateHeartbeat);
    runWithNewCluster(3, this::runTestStreamObserverCleanupOnFollowerKillRestart);
  }

  private void runTestStreamObserverCleanupOnFollowerKillRestart(MiniRaftClusterWithGrpc cluster) throws Exception {
    final RaftServer.Division leader = waitForLeader(cluster);
    final RaftPeerId leaderId = leader.getId();
    final int numCycles = 5;

    try (RaftClient client = cluster.createClient(leaderId)) {
      for (int i = 0; i < 5; i++) {
        Assertions.assertTrue(client.io().send(
            new RaftTestUtil.SimpleMessage("init-" + i)).isSuccess());
      }
    }

    for (int cycle = 0; cycle < numCycles; cycle++) {
      LOG.info("=== Kill/Restart cycle {} ===", cycle);
      final RaftPeerId followerId = cluster.getFollowers().get(0).getId();

      cluster.killServer(followerId);

      try (RaftClient client = cluster.createClient(leaderId)) {
        for (int i = 0; i < 5; i++) {
          Assertions.assertTrue(client.io().send(
              new RaftTestUtil.SimpleMessage("cycle" + cycle + "-" + i)).isSuccess());
        }
      }

      cluster.restartServer(followerId, false);

      JavaUtils.attempt(() -> {
        final RaftServer.Division f = cluster.getDivision(followerId);
        Assertions.assertTrue(f.getInfo().getLastAppliedIndex() > 0,
            "Follower " + followerId + " should have applied entries");
      }, 10, ONE_SECOND, "follower-catchup-" + cycle, LOG);
    }

    // Verify all entries committed across the cluster
    try (RaftClient client = cluster.createClient(leaderId)) {
      final RaftClientReply reply = client.io().send(new RaftTestUtil.SimpleMessage("final"));
      Assertions.assertTrue(reply.isSuccess());
      client.io().watch(reply.getLogIndex(), RaftProtos.ReplicationLevel.ALL_COMMITTED);
    }
  }
}
