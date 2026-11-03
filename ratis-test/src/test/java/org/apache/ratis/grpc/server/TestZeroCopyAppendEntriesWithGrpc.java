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
package org.apache.ratis.grpc.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;
import org.apache.ratis.BaseTest;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.MiniRaftClusterWithGrpc;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.server.raftlog.LogProtoUtils;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.Test;

/**
 * Zero-copy appendEntries on followers (raft.grpc.server.zero-copy.append-entries.enabled), with a state machine that
 * keeps its data out of the log, like Apache Ozone: every follower must write the same state machine data as the
 * leader, entry by entry.
 */
public class TestZeroCopyAppendEntriesWithGrpc extends BaseTest {
  private static final int MESSAGES = 30;
  private static final int MESSAGE_SIZE = 256 << 10;

  /**
   * The data is state machine data. {@link #write} checksums it and keeps nothing else of a follower's entry, which
   * zero-copy appendEntries requires; the leader keeps its own data for {@link #read}.
   */
  public static class ChecksumStateMachine extends BaseStateMachine implements StateMachine.DataApi {
    private final Map<Long, ByteString> leaderData = new ConcurrentHashMap<>();
    private final Map<Long, Long> checksums = new ConcurrentHashMap<>();

    @Override
    public DataApi data() {
      return this;
    }

    @Override
    public TransactionContext startTransaction(RaftClientRequest request) {
      return TransactionContext.newBuilder().setStateMachine(this).setClientRequest(request)
          .setLogData(ByteString.copyFromUtf8("write")).setStateMachineData(request.getMessage().getContent())
          .build();
    }

    @Override
    public TransactionContext startTransaction(LogEntryProto entry, RaftPeerRole role) {
      // a follower's transaction must not keep the data, which may be released after it is written
      return TransactionContext.newBuilder().setStateMachine(this).setServerRole(role)
          .setLogEntry(LogProtoUtils.removeStateMachineData(entry)).build();
    }

    @Override
    public CompletableFuture<?> write(LogEntryProto entry, TransactionContext context) {
      final ByteString data = entry.getStateMachineLogEntry().getStateMachineEntry().getStateMachineData();
      final CRC32 crc = new CRC32();
      for (ByteBuffer b : data.asReadOnlyByteBufferList()) {
        crc.update(b);
      }
      checksums.put(entry.getIndex(), crc.getValue());
      if (context != null && context.getServerRole() == RaftPeerRole.LEADER) {
        leaderData.put(entry.getIndex(), data);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<ByteString> read(LogEntryProto entry, TransactionContext context) {
      final ByteString data = leaderData.get(entry.getIndex());
      return data != null ? CompletableFuture.completedFuture(data)
          : CompletableFuture.supplyAsync(() -> {
            throw new IllegalStateException("No data at index " + entry.getIndex());
          });
    }

    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
      final LogEntryProto entry = trx.getLogEntry();
      updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
      return CompletableFuture.completedFuture(Message.valueOf("OK"));
    }
  }

  private static RaftProperties newProperties(boolean zeroCopy, boolean caching) {
    final RaftProperties properties = new RaftProperties();
    properties.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY, ChecksumStateMachine.class, StateMachine.class);
    RaftServerConfigKeys.Log.StateMachineData.setCachingEnabled(properties, caching);
    GrpcConfigKeys.Server.setZeroCopyAppendEntriesEnabled(properties, zeroCopy);
    return properties;
  }

  private static boolean isZeroCopy(RaftServer server) {
    return ((GrpcServicesImpl) server.getServerRpc()).isZeroCopyAppendEntries();
  }

  private static Map<Long, Long> getChecksums(MiniRaftCluster cluster, RaftPeerId id, RaftGroupId groupId)
      throws Exception {
    return ((ChecksumStateMachine) cluster.getDivision(id, groupId).getStateMachine()).checksums;
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testFollowersWriteTheLeadersData(boolean zeroCopy) throws Exception {
    final MiniRaftCluster cluster = MiniRaftClusterWithGrpc.FACTORY.newCluster(3, newProperties(zeroCopy, true));
    try {
      cluster.start();
      for (RaftServer server : cluster.getServers()) {
        assertEquals(zeroCopy, isZeroCopy(server));
      }
      final RaftPeerId leaderId = RaftTestUtil.waitForLeader(cluster).getId();
      final RaftGroupId groupId = cluster.getGroupId();

      final byte[] bytes = new byte[MESSAGE_SIZE];
      final Random random = new Random(2);
      try (RaftClient client = cluster.createClient(leaderId)) {
        for (int i = 0; i < MESSAGES; i++) {
          random.nextBytes(bytes);
          assertTrue(client.io().send(Message.valueOf(ByteString.copyFrom(bytes))).isSuccess());
        }
      }

      final Map<Long, Long> leader = getChecksums(cluster, leaderId, groupId);
      assertTrue(leader.size() >= MESSAGES, "leader wrote " + leader.size());
      for (RaftServer server : cluster.getServers()) {
        final RaftPeerId id = server.getId();
        if (id.equals(leaderId)) {
          continue;
        }
        final Map<Long, Long> follower = getChecksums(cluster, id, groupId);
        // a follower may still be writing the last entries the majority has committed
        for (int i = 0; i < 100 && !follower.keySet().containsAll(leader.keySet()); i++) {
          TimeUnit.MILLISECONDS.sleep(100);
        }
        for (Map.Entry<Long, Long> e : leader.entrySet()) {
          assertEquals(e.getValue(), follower.get(e.getKey()), id + ": state machine data at index " + e.getKey());
        }
      }
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testRequiresStateMachineDataCaching() throws Exception {
    // without caching, the log cache would keep the data that zero-copy releases
    final MiniRaftCluster cluster = MiniRaftClusterWithGrpc.FACTORY.newCluster(3, newProperties(true, false));
    try {
      cluster.start();
      for (RaftServer server : cluster.getServers()) {
        assertFalse(isZeroCopy(server));
      }
    } finally {
      cluster.shutdown();
    }
  }
}
