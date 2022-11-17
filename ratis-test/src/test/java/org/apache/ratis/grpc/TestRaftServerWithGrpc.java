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

import static org.apache.ratis.server.metrics.RaftServerMetricsImpl.RAFT_CLIENT_READ_REQUEST;
import static org.apache.ratis.server.metrics.RaftServerMetricsImpl.RAFT_CLIENT_STALE_READ_REQUEST;
import static org.apache.ratis.server.metrics.RaftServerMetricsImpl.RAFT_CLIENT_WATCH_REQUEST;
import static org.apache.ratis.server.metrics.RaftServerMetricsImpl.RAFT_CLIENT_WRITE_REQUEST;

import org.apache.ratis.metrics.RatisMetricRegistry;
import org.slf4j.event.Level;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.security.SecurityTestUtils;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.BaseTest;
import org.apache.ratis.metrics.impl.DefaultTimekeeperImpl;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.RaftTestUtil.SimpleMessage;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.client.RaftClientRpc;
import org.apache.ratis.client.impl.RaftClientTestUtil;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.client.GrpcClientProtocolClient;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.protocol.exceptions.AlreadyClosedException;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.TimeoutIOException;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.RaftServerRpc;
import org.apache.ratis.server.impl.RaftServerTestUtil;
import org.apache.ratis.server.metrics.RaftServerMetricsImpl;
import org.apache.ratis.statemachine.impl.SimpleStateMachine4Testing;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.Slf4jUtils;
import org.apache.ratis.util.ProtoUtils;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.net.ssl.KeyManager;
import javax.net.ssl.TrustManager;
import java.io.IOException;
import java.nio.channels.OverlappingFileLockException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@RunWith(Parameterized.class)
public class TestRaftServerWithGrpc extends BaseTest implements MiniRaftClusterWithGrpc.FactoryGet {
  {
    Slf4jUtils.setLogLevel(GrpcClientProtocolClient.LOG, Level.TRACE);
  }

  public TestRaftServerWithGrpc(Boolean separateHeartbeat) {
    GrpcConfigKeys.Server.setHeartbeatChannel(getProperties(), separateHeartbeat);
  }

  @Parameterized.Parameters
  public static Collection<Boolean[]> data() {
    return Arrays.asList((new Boolean[][] {{Boolean.FALSE}, {Boolean.TRUE}}));
  }

  @Before
  public void setup() {
    final RaftProperties p = getProperties();
    p.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY, SimpleStateMachine4Testing.class, StateMachine.class);
    RaftClientConfigKeys.Rpc.setRequestTimeout(p, TimeDuration.valueOf(1, TimeUnit.SECONDS));
  }

  @Test
  public void testServerRestartOnException() throws Exception {
    runWithNewCluster(1, this::runTestServerRestartOnException);
  }

  static RaftServer newRaftServer(MiniRaftClusterWithGrpc cluster, RaftPeerId id, StateMachine stateMachine,
      RaftStorage.StartupOption option, RaftProperties p) throws IOException {
    final RaftGroup group = cluster.getGroup();
    return RaftServer.newBuilder()
        .setServerId(id)
        .setGroup(cluster.getGroup())
        .setStateMachine(stateMachine)
        .setOption(option)
        .setProperties(p)
        .setParameters(cluster.setPropertiesAndInitParameters(id, group, p))
        .build();
  }

  void runTestServerRestartOnException(MiniRaftClusterWithGrpc cluster) throws Exception {
    final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster);
    final RaftPeerId leaderId = leader.getId();

    final RaftProperties p = getProperties();
    GrpcConfigKeys.Server.setPort(p, RaftServerTestUtil.getServerRpc(leader).getInetSocketAddress().getPort());

    // Create a raft server proxy with server rpc bound to a different address
    // compared to leader. This helps in locking the raft storage directory to
    // be used by next raft server proxy instance.
    final StateMachine stateMachine = cluster.getLeader().getStateMachine();
    RaftServerConfigKeys.setStorageDir(p, Collections.singletonList(cluster.getStorageDir(leaderId)));
    newRaftServer(cluster, leaderId, stateMachine, RaftStorage.StartupOption.FORMAT, p);
    // Close the server rpc for leader so that new raft server can be bound to it.
    RaftServerTestUtil.getServerRpc(cluster.getLeader()).close();

    // Create a raft server proxy with server rpc bound to same address as
    // the leader. This step would fail as the raft storage has been locked by
    // the raft server proxy created earlier. Raft server proxy should close
    // the rpc server on failure.
    RaftServerConfigKeys.setStorageDir(p, Collections.singletonList(cluster.getStorageDir(leaderId)));
    testFailureCase("Starting a new server with the same address should fail",
        () -> newRaftServer(cluster, leaderId, stateMachine, RaftStorage.StartupOption.RECOVER, p).start(),
        CompletionException.class, LOG, IOException.class, OverlappingFileLockException.class);

    // Try to start a raft server rpc at the leader address.
    cluster.getServerFactory(leaderId).newRaftServerRpc(cluster.getServer(leaderId));
  }

  @Test
  public void testUnsupportedMethods() throws Exception {
    runWithNewCluster(1, this::runTestUnsupportedMethods);
  }

  void runTestUnsupportedMethods(MiniRaftClusterWithGrpc cluster) throws Exception {
    final RaftPeerId leaderId = RaftTestUtil.waitForLeader(cluster).getId();
    final RaftServerRpc rpc = cluster.getServerFactory(leaderId).newRaftServerRpc(cluster.getServer(leaderId));

    testFailureCase("appendEntries",
        () -> rpc.appendEntries(null),
        UnsupportedOperationException.class);

    testFailureCase("installSnapshot",
        () -> rpc.installSnapshot(null),
        UnsupportedOperationException.class);
  }

  @Test
  public void testLeaderRestart() throws Exception {
    runWithNewCluster(3, this::runTestLeaderRestart);
  }

  void runTestLeaderRestart(MiniRaftClusterWithGrpc cluster) throws Exception {
    final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster);

    try (final RaftClient client = cluster.createClient()) {
      // send a request to make sure leader is ready
      final CompletableFuture<RaftClientReply> f = client.async().send(new SimpleMessage("testing"));
      Assert.assertTrue(f.get().isSuccess());
    }

    try (final RaftClient client = cluster.createClient()) {
      final RaftClientRpc rpc = client.getClientRpc();

      final AtomicLong seqNum = new AtomicLong();
      {
        // send a request using rpc directly
        final RaftClientRequest request = newRaftClientRequest(client, leader.getId(), seqNum.incrementAndGet());
        final CompletableFuture<RaftClientReply> f = rpc.sendRequestAsync(request);
        Assert.assertTrue(f.get().isSuccess());
      }

      // send another request which will be blocked
      final SimpleStateMachine4Testing stateMachine = SimpleStateMachine4Testing.get(leader);
      stateMachine.blockStartTransaction();
      final RaftClientRequest requestBlocked = newRaftClientRequest(client, leader.getId(), seqNum.incrementAndGet());
      final CompletableFuture<RaftClientReply> futureBlocked = rpc.sendRequestAsync(requestBlocked);

      // change leader
      RaftTestUtil.changeLeader(cluster, leader.getId());
      Assert.assertNotEquals(RaftPeerRole.LEADER, leader.getInfo().getCurrentRole());

      // the blocked request should fail
      testFailureCase("request should fail", futureBlocked::get,
          ExecutionException.class, AlreadyClosedException.class);
      stateMachine.unblockStartTransaction();

      // send one more request which should timeout.
      final RaftClientRequest requestTimeout = newRaftClientRequest(client, leader.getId(), seqNum.incrementAndGet());
      rpc.handleException(leader.getId(), new Exception(), true);
      final CompletableFuture<RaftClientReply> f = rpc.sendRequestAsync(requestTimeout);
      testFailureCase("request should timeout", f::get,
          ExecutionException.class, TimeoutIOException.class);
    }

  }

  @Test
  public void testRaftClientMetrics() throws Exception {
    runWithNewCluster(3, this::testRaftClientRequestMetrics);
  }

  @Test
  public void testRaftServerMetrics() throws Exception {
    final RaftProperties p = getProperties();
    RaftServerConfigKeys.Write.setElementLimit(p, 10);
    RaftServerConfigKeys.Write.setByteLimit(p, SizeInBytes.valueOf("1MB"));
    try {
      runWithNewCluster(3, this::testRequestMetrics);
    } finally {
      RaftServerConfigKeys.Write.setElementLimit(p, RaftServerConfigKeys.Write.ELEMENT_LIMIT_DEFAULT);
      RaftServerConfigKeys.Write.setByteLimit(p, RaftServerConfigKeys.Write.BYTE_LIMIT_DEFAULT);
    }
  }

  void testRequestMetrics(MiniRaftClusterWithGrpc cluster) throws Exception {

    try (RaftClient client = cluster.createClient()) {
      // send a request to make sure leader is ready
      final CompletableFuture< RaftClientReply > f = client.async().send(new SimpleMessage("testing"));
      Assert.assertTrue(f.get().isSuccess());
    }

    SimpleStateMachine4Testing stateMachine = SimpleStateMachine4Testing.get(cluster.getLeader());
    stateMachine.blockFlushStateMachineData();

    // Block stateMachine flush data, so that 2nd request will not be
    // completed, and so it will not be removed from pending request map.
    List<RaftClient> clients = new ArrayList<>();

    try {
      RaftClient client = cluster.createClient(cluster.getLeader().getId(), RetryPolicies.noRetry());
      clients.add(client);
      client.async().send(new SimpleMessage("2nd Message"));

      for (int i = 0; i < 10; i++) {
        client = cluster.createClient(cluster.getLeader().getId(), RetryPolicies.noRetry());
        clients.add(client);
        client.async().send(new SimpleMessage("message " + i));
      }

      // Because we have passed 11 requests, and the element queue size is 10.
      RaftTestUtil.waitFor(() -> getRaftServerMetrics(cluster.getLeader())
          .getNumRequestQueueLimitHits().getCount() == 1, 300, 5000);

      stateMachine.unblockFlushStateMachineData();

      // Send a message with 1025kb , our byte size limit is 1024kb (1mb) , so it should fail
      // and byte size counter limit will be hit.

      client = cluster.createClient(cluster.getLeader().getId(), RetryPolicies.noRetry());
      final SizeInBytes size = SizeInBytes.valueOf("1025kb");
      final ByteString bytes = randomByteString(size.getSizeInt());
      Assert.assertEquals(size.getSizeInt(), bytes.size());
      client.async().send(new SimpleMessage(size + "-message", bytes));
      clients.add(client);

      RaftTestUtil.waitFor(() -> getRaftServerMetrics(cluster.getLeader())
          .getNumRequestsByteSizeLimitHits().getCount() == 1, 300, 5000);

      Assert.assertEquals(2, getRaftServerMetrics(cluster.getLeader())
          .getNumResourceLimitHits().getCount());
    } finally {
      for (RaftClient client : clients) {
        client.close();
      }
    }
  }
  
  static ByteString randomByteString(int size) {
    final ByteString.Output out = ByteString.newOutput(size);
    final ThreadLocalRandom random = ThreadLocalRandom.current();
    final byte[] buffer = new byte[4096];
    for(; size > 0; ) {
      random.nextBytes(buffer);
      final int n = Math.min(size, buffer.length);
      out.write(buffer, 0, n);
      size -= n;
    }
    return out.toByteString();
  }

  static RaftServerMetricsImpl getRaftServerMetrics(RaftServer.Division division) {
    return (RaftServerMetricsImpl) division.getRaftServerMetrics();
  }

  void testRaftClientRequestMetrics(MiniRaftClusterWithGrpc cluster) throws IOException,
      ExecutionException, InterruptedException {
    final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster);
    final RaftServerMetricsImpl raftServerMetrics = getRaftServerMetrics(leader);
    final RatisMetricRegistry registry = raftServerMetrics.getRegistry();

    try (final RaftClient client = cluster.createClient()) {
      final CompletableFuture<RaftClientReply> f1 = client.async().send(new SimpleMessage("testing"));
      Assert.assertTrue(f1.get().isSuccess());
      final DefaultTimekeeperImpl write = (DefaultTimekeeperImpl) registry.timer(RAFT_CLIENT_WRITE_REQUEST);
      Assert.assertTrue(write.getTimer().getCount() > 0);

      final CompletableFuture<RaftClientReply> f2 = client.async().sendReadOnly(new SimpleMessage("testing"));
      Assert.assertTrue(f2.get().isSuccess());
      final DefaultTimekeeperImpl read = (DefaultTimekeeperImpl) registry.timer(RAFT_CLIENT_READ_REQUEST);
      Assert.assertTrue(read.getTimer().getCount() > 0);

      final CompletableFuture<RaftClientReply> f3 = client.async().sendStaleRead(new SimpleMessage("testing"),
          0, leader.getId());
      Assert.assertTrue(f3.get().isSuccess());
      final DefaultTimekeeperImpl staleRead = (DefaultTimekeeperImpl) registry.timer(RAFT_CLIENT_STALE_READ_REQUEST);
      Assert.assertTrue(staleRead.getTimer().getCount() > 0);

      final CompletableFuture<RaftClientReply> f4 = client.async().watch(0, RaftProtos.ReplicationLevel.ALL);
      Assert.assertTrue(f4.get().isSuccess());
      final DefaultTimekeeperImpl watchAll = (DefaultTimekeeperImpl) registry.timer(
          String.format(RAFT_CLIENT_WATCH_REQUEST, "-ALL"));
      Assert.assertTrue(watchAll.getTimer().getCount() > 0);

      final CompletableFuture<RaftClientReply> f5 = client.async().watch(0, RaftProtos.ReplicationLevel.MAJORITY);
      Assert.assertTrue(f5.get().isSuccess());
      final DefaultTimekeeperImpl watch = (DefaultTimekeeperImpl) registry.timer(
          String.format(RAFT_CLIENT_WATCH_REQUEST, ""));
      Assert.assertTrue(watch.getTimer().getCount() > 0);
    }
  }

  static RaftClientRequest newRaftClientRequest(RaftClient client, RaftPeerId serverId, long seqNum) {
    final SimpleMessage m = new SimpleMessage("m" + seqNum);
    return RaftClientTestUtil.newRaftClientRequest(client, serverId, seqNum, m,
        RaftClientRequest.writeRequestType(), ProtoUtils.toSlidingWindowEntry(seqNum, seqNum == 1L));
  }

  @Test
  public void testTlsWithKeyAndTrustManager() throws Exception {
    final RaftProperties p = getProperties();
    RaftServerConfigKeys.Write.setElementLimit(p, 10);
    RaftServerConfigKeys.Write.setByteLimit(p, SizeInBytes.valueOf("1MB"));
    String[] ids = MiniRaftCluster.generateIds(3, 3);

    KeyManager serverKeyManager = SecurityTestUtils.getKeyManager(SecurityTestUtils::getServerKeyStore);
    TrustManager serverTrustManager = SecurityTestUtils.getTrustManager(SecurityTestUtils::getTrustStore);
    KeyManager clientKeyManager = SecurityTestUtils.getKeyManager(SecurityTestUtils::getClientKeyStore);
    TrustManager clientTrustManager = SecurityTestUtils.getTrustManager(SecurityTestUtils::getTrustStore);

    GrpcTlsConfig serverConfig = new GrpcTlsConfig(serverKeyManager, serverTrustManager, true);
    GrpcTlsConfig clientConfig = new GrpcTlsConfig(clientKeyManager, clientTrustManager, true);

    final Parameters parameters = new Parameters();
    GrpcConfigKeys.Server.setTlsConf(parameters, serverConfig);
    GrpcConfigKeys.Admin.setTlsConf(parameters, clientConfig);
    GrpcConfigKeys.Client.setTlsConf(parameters, clientConfig);

    MiniRaftClusterWithGrpc cluster = null;
    try {
      cluster = new MiniRaftClusterWithGrpc(ids, new String[0], p, parameters);
      cluster.start();
      testRequestMetrics(cluster);
    }  finally {
      RaftServerConfigKeys.Write.setElementLimit(p, RaftServerConfigKeys.Write.ELEMENT_LIMIT_DEFAULT);
      RaftServerConfigKeys.Write.setByteLimit(p, RaftServerConfigKeys.Write.BYTE_LIMIT_DEFAULT);
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
