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
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.server.GrpcServicesImpl;
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.proto.RaftProtos.ReplicationLevel;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.security.SecurityTestUtils;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.server.impl.PeerChanges;
import org.apache.ratis.server.impl.RaftServerTestUtil;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachine4Testing;
import org.apache.ratis.util.CodeInjectionForTesting;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.SizeInBytes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.net.ssl.KeyManager;
import javax.net.ssl.TrustManager;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class TestGrpcDataTransferEventListener extends BaseTest {
  private static RaftProperties newProperties() {
    final RaftProperties properties = new RaftProperties();
    properties.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        SimpleStateMachine4Testing.class, StateMachine.class);
    return properties;
  }

  private static Parameters newParameters(boolean tls,
      ConcurrentLinkedQueue<GrpcDataTransferEvent> events) throws Exception {
    final Parameters parameters = new Parameters();
    GrpcConfigKeys.Server.setDataTransferEventConsumer(parameters, events::add);
    if (tls) {
      final KeyManager serverKeyManager =
          SecurityTestUtils.getKeyManager(SecurityTestUtils::getServerKeyStore);
      final TrustManager serverTrustManager =
          SecurityTestUtils.getTrustManager(SecurityTestUtils::getTrustStore);
      final KeyManager clientKeyManager =
          SecurityTestUtils.getKeyManager(SecurityTestUtils::getClientKeyStore);
      final TrustManager clientTrustManager =
          SecurityTestUtils.getTrustManager(SecurityTestUtils::getTrustStore);

      GrpcConfigKeys.Server.setTlsConf(parameters,
          new GrpcTlsConfig(serverKeyManager, serverTrustManager, true));
      final GrpcTlsConfig clientConfig =
          new GrpcTlsConfig(clientKeyManager, clientTrustManager, true);
      GrpcConfigKeys.Admin.setTlsConf(parameters, clientConfig);
      GrpcConfigKeys.Client.setTlsConf(parameters, clientConfig);
    }
    return parameters;
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  public void testSuccessfulAppendEntries(boolean tls) throws Exception {
    final ConcurrentLinkedQueue<GrpcDataTransferEvent> events = new ConcurrentLinkedQueue<>();
    final Parameters parameters = newParameters(tls, events);
    final String[] ids = MiniRaftCluster.generateIds(3, 10);

    try (MiniRaftClusterWithGrpc cluster =
        new MiniRaftClusterWithGrpc(ids, new String[0], newProperties(), parameters)) {
      cluster.start();
      final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster);
      final RaftPeerId leaderId = leader.getId();
      events.clear();

      try (RaftClient client = cluster.createClient(leaderId)) {
        final RaftClientReply write =
            client.io().send(new RaftTestUtil.SimpleMessage("data-transfer"));
        Assertions.assertTrue(write.isSuccess());
        Assertions.assertTrue(
            client.io().watch(write.getLogIndex(), ReplicationLevel.ALL_COMMITTED).isSuccess());
      }

      final Set<RaftPeerId> followers = cluster.getFollowers().stream()
          .map(RaftServer.Division::getId)
          .collect(Collectors.toSet());
      JavaUtils.attempt(() -> {
        final Set<RaftPeerId> destinations = successfulEvents(events, leaderId).stream()
            .map(GrpcDataTransferEvent::getDestination)
            .collect(Collectors.toSet());
        Assertions.assertEquals(followers, destinations);
      }, 10, HUNDRED_MILLIS, "data transfer events", LOG);

      final GrpcDataTransferEvent.ProtectionMethod expected = tls
          ? GrpcDataTransferEvent.ProtectionMethod.TLS
          : GrpcDataTransferEvent.ProtectionMethod.NONE;
      successfulEvents(events, leaderId).forEach(event -> {
        Assertions.assertNotNull(event.getTimestamp());
        Assertions.assertEquals(expected, event.getProtectionMethod());
        Assertions.assertNull(event.getError());
      });
    }
  }

  private static List<GrpcDataTransferEvent> successfulEvents(
      ConcurrentLinkedQueue<GrpcDataTransferEvent> events, RaftPeerId source) {
    return events.stream()
        .filter(event -> event.getSource().equals(source))
        .filter(event -> event.getResult() == GrpcDataTransferEvent.Result.SUCCESS)
        .collect(Collectors.toList());
  }

  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS)
  public void testSuccessfulInstallSnapshot() throws Exception {
    final ConcurrentLinkedQueue<GrpcDataTransferEvent> events = new ConcurrentLinkedQueue<>();
    final Parameters parameters = newParameters(false, events);
    final RaftProperties properties = newProperties();
    RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(properties, true);
    RaftServerConfigKeys.Snapshot.setAutoTriggerThreshold(properties, 64);
    RaftServerConfigKeys.Log.setPurgeGap(properties, 8);
    RaftServerConfigKeys.Log.Appender.setSnapshotChunkSizeMax(properties, SizeInBytes.ONE_KB);
    RaftServerConfigKeys.LeaderElection.setMemberMajorityAdd(properties, true);

    final String[] ids = MiniRaftCluster.generateIds(1, 30);
    try (MiniRaftClusterWithGrpc cluster =
        new MiniRaftClusterWithGrpc(ids, new String[0], properties, parameters)) {
      cluster.start();
      final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster);
      final RaftPeerId leaderId = leader.getId();

      try (RaftClient client = cluster.createClient(leaderId)) {
        for (int i = 0; i < 127; i++) {
          Assertions.assertTrue(
              client.io().send(new RaftTestUtil.SimpleMessage("snapshot-" + i)).isSuccess());
        }
        Assertions.assertTrue(client.getSnapshotManagementApi(leaderId).create(3000).isSuccess());
      }
      final SnapshotInfo leaderSnapshot = leader.getStateMachine().getLatestSnapshot();
      Assertions.assertNotNull(leaderSnapshot);
      events.clear();

      final PeerChanges change = cluster.addNewPeers(1, true);
      final RaftPeerId addedPeer = change.getAddedPeers().get(0).getId();
      cluster.setConfiguration(change.getPeersInNewConf());
      RaftServerTestUtil.waitAndCheckNewConf(cluster, change.getPeersInNewConf(), 0, null);

      JavaUtils.attempt(() -> {
        final SnapshotInfo installed =
            cluster.getDivision(addedPeer).getStateMachine().getLatestSnapshot();
        Assertions.assertNotNull(installed);
        Assertions.assertTrue(successfulEvents(events, leaderId).stream()
            .anyMatch(event -> event.getDestination().equals(addedPeer)));
      }, 20, ONE_SECOND, "snapshot data transfer event", LOG);
    }
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  public void testAppendEntriesFailureAndConsumerException() throws Exception {
    final ConcurrentLinkedQueue<GrpcDataTransferEvent> events = new ConcurrentLinkedQueue<>();
    final AtomicInteger consumerCalls = new AtomicInteger();
    final Parameters parameters = new Parameters();
    GrpcConfigKeys.Server.setDataTransferEventConsumer(parameters, event -> {
      events.add(event);
      consumerCalls.incrementAndGet();
      if (event.getResult() == GrpcDataTransferEvent.Result.FAILURE) {
        throw new IllegalStateException("Injected consumer failure");
      }
    });

    final String[] ids = MiniRaftCluster.generateIds(2, 20);
    try (MiniRaftClusterWithGrpc cluster =
        new MiniRaftClusterWithGrpc(ids, new String[0], newProperties(), parameters)) {
      cluster.start();
      final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster);
      final RaftPeerId leaderId = leader.getId();
      events.clear();
      consumerCalls.set(0);

      final AtomicBoolean injectFailure = new AtomicBoolean(true);
      try {
        CodeInjectionForTesting.put(
            GrpcServicesImpl.GRPC_SEND_SERVER_REQUEST, (localId, remoteId, args) -> {
              if (leaderId.equals(localId)
                  && args.length > 0
                  && args[0] instanceof AppendEntriesRequestProto) {
                final AppendEntriesRequestProto request = (AppendEntriesRequestProto) args[0];
                final boolean containsStateMachineData = request.getEntriesList().stream()
                    .anyMatch(entry -> entry.hasStateMachineLogEntry());
                if (containsStateMachineData && injectFailure.compareAndSet(true, false)) {
                  throw new IllegalStateException("Injected AppendEntries failure");
                }
              }
              return false;
            });

        try (RaftClient client = cluster.createClient(leaderId)) {
          Assertions.assertTrue(
              client.io().send(new RaftTestUtil.SimpleMessage("retry-after-failure")).isSuccess());
        }

        JavaUtils.attempt(() -> Assertions.assertTrue(events.stream()
                .anyMatch(event -> event.getResult() == GrpcDataTransferEvent.Result.FAILURE)),
            10, HUNDRED_MILLIS, "failed data transfer event", LOG);
      } finally {
        CodeInjectionForTesting.remove(GrpcServicesImpl.GRPC_SEND_SERVER_REQUEST);
      }

      final GrpcDataTransferEvent failure = events.stream()
          .filter(event -> event.getResult() == GrpcDataTransferEvent.Result.FAILURE)
          .findFirst()
          .orElseThrow(AssertionError::new);
      Assertions.assertEquals(leaderId, failure.getSource());
      Assertions.assertEquals(GrpcDataTransferEvent.ProtectionMethod.NONE,
          failure.getProtectionMethod());
      Assertions.assertTrue(failure.getError() instanceof IllegalStateException);
      Assertions.assertEquals(1, events.stream()
          .filter(event -> event.getResult() == GrpcDataTransferEvent.Result.FAILURE)
          .count());
      Assertions.assertTrue(consumerCalls.get() > 0);
    }
  }
}
