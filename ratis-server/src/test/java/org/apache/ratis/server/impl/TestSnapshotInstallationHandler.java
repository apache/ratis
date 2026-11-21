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
package org.apache.ratis.server.impl;

import org.apache.ratis.client.impl.ClientProtoUtils;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotReplyProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotRequestProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotResult;
import org.apache.ratis.proto.RaftProtos.ReadIndexRequestProto;
import org.apache.ratis.proto.RaftProtos.ReadRequestTypeProto;
import org.apache.ratis.proto.RaftProtos.RoleInfoProto;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.ReadException;
import org.apache.ratis.server.DivisionInfo;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.RaftServerRpc;
import org.apache.ratis.server.impl.FollowerState.UpdateType;
import org.apache.ratis.server.protocol.RaftServerAsynchronousProtocol;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.JavaUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

public class TestSnapshotInstallationHandler {
  private static final long TERM = 1;
  private static final long FIRST_AVAILABLE_INDEX = 10;
  private static final long APPLIED_INDEX = 100;

  private final RaftGroupId groupId = RaftGroupId.randomId();
  private final RaftPeerId leaderId = RaftPeerId.valueOf("leader");
  private final RaftGroupMemberId memberId = RaftGroupMemberId.valueOf(RaftPeerId.valueOf("follower"), groupId);
  private final RaftServerImpl server = Mockito.mock(RaftServerImpl.class);
  private final ServerState state = Mockito.mock(ServerState.class);
  private final RaftLog log = Mockito.mock(RaftLog.class);
  private final StateMachine stateMachine = Mockito.mock(StateMachine.class);
  private final StateMachine.FollowerEventApi followerEvent = Mockito.mock(StateMachine.FollowerEventApi.class);
  private final CompletableFuture<TermIndex> snapshotFuture = new CompletableFuture<>();
  private final Message query = Message.valueOf("query");

  private SnapshotInstallationHandler handler;
  private ReadRequestHandler readHandler;
  private ReadRequests readRequests;

  @BeforeEach
  public void setup() throws Exception {
    final RaftProperties properties = new RaftProperties();
    RaftServerConfigKeys.Log.Appender.setInstallSnapshotEnabled(properties, false);
    RaftServerConfigKeys.Read.setOption(properties, RaftServerConfigKeys.Read.Option.LINEARIZABLE);
    Mockito.when(server.getState()).thenReturn(state);
    Mockito.when(server.getStateMachine()).thenReturn(stateMachine);
    Mockito.when(server.getId()).thenReturn(memberId.getPeerId());
    Mockito.when(state.getMemberId()).thenReturn(memberId);
    Mockito.when(state.getLog()).thenReturn(log);
    Mockito.when(state.getCurrentTerm()).thenReturn(TERM);
    Mockito.when(state.recognizeLeader(UpdateType.INSTALL_SNAPSHOT_NOTIFICATION, leaderId, TERM)).thenReturn(true);
    Mockito.when(server.changeToFollowerAndPersistMetadata(TERM, true, UpdateType.INSTALL_SNAPSHOT_NOTIFICATION))
        .thenReturn(CompletableFuture.completedFuture(null));
    Mockito.when(server.getRoleInfoProto()).thenReturn(RoleInfoProto.getDefaultInstance());
    Mockito.when(stateMachine.followerEvent()).thenReturn(followerEvent);
    Mockito.when(stateMachine.event()).thenReturn(Mockito.mock(StateMachine.EventApi.class));
    Mockito.when(followerEvent.notifyInstallSnapshotFromLeader(Mockito.any(), Mockito.any())).thenReturn(snapshotFuture);
    Mockito.when(stateMachine.query(query)).thenReturn(CompletableFuture.completedFuture(query));

    final RoleInfo role = Mockito.mock(RoleInfo.class);
    final DivisionInfo info = Mockito.mock(DivisionInfo.class);
    final RaftServerRpc rpc = Mockito.mock(RaftServerRpc.class);
    final RaftServerAsynchronousProtocol async = Mockito.mock(RaftServerAsynchronousProtocol.class);
    Mockito.when(server.getRole()).thenReturn(role);
    Mockito.when(server.getInfo()).thenReturn(info);
    Mockito.when(info.getLeaderId()).thenReturn(leaderId);
    Mockito.when(server.getServerRpc()).thenReturn(rpc);
    Mockito.when(rpc.async()).thenReturn(async);
    Mockito.when(async.readIndexAsync(Mockito.any(ReadIndexRequestProto.class))).thenReturn(
        CompletableFuture.completedFuture(ServerProtoUtils.toReadIndexReplyProto(memberId.getPeerId(),
            RaftGroupMemberId.valueOf(leaderId, groupId), true, APPLIED_INDEX)));

    readRequests = new ReadRequests(APPLIED_INDEX, properties);
    Mockito.when(state.getReadRequests()).thenReturn(readRequests);
    readHandler = new ReadRequestHandler(server, properties);
    Mockito.when(server.getReadRequestHandler()).thenReturn(readHandler);
    handler = new SnapshotInstallationHandler(server, properties);
    Mockito.when(server.getSnapshotInstallationHandler()).thenReturn(handler);
  }

  private InstallSnapshotReplyProto notifyInstallSnapshot() throws Exception {
    return handler.installSnapshot(InstallSnapshotRequestProto.newBuilder()
        .setServerRequest(ClientProtoUtils.toRaftRpcRequestProtoBuilder(
            RaftGroupMemberId.valueOf(leaderId, groupId), memberId.getPeerId()))
        .setLeaderTerm(TERM)
        .setNotification(InstallSnapshotRequestProto.NotificationProto.newBuilder()
            .setFirstAvailableTermIndex(TermIndex.valueOf(TERM, FIRST_AVAILABLE_INDEX).toProto()))
        .build());
  }

  private CompletableFuture<Message> readOnlyAsync() throws Exception {
    return readHandler.readOnlyAsync(ClientId.randomId(), ReadRequestTypeProto.getDefaultInstance(), query);
  }

  @ParameterizedTest
  @ValueSource(longs = {9, 10, 20})
  public void testAlreadyInstalledDoesNotRejectReads(long snapshotIndex) throws Exception {
    final CompletableFuture<Long> pendingRead = readRequests.waitToAdvance(APPLIED_INDEX + 1, () -> null);
    Mockito.when(log.getSnapshotIndex()).thenAnswer(invocation -> {
      // Run a follower read while the notification checks coverage, before it can return ALREADY_INSTALLED.
      final Message reply = CompletableFuture.supplyAsync(() ->
          JavaUtils.supplyAndWrapAsCompletionException(() -> readOnlyAsync().join())).get(5, TimeUnit.SECONDS);
      Assertions.assertSame(query, reply);
      Assertions.assertEquals(RaftLog.INVALID_LOG_INDEX, handler.getInProgressInstallSnapshotIndex());
      return snapshotIndex;
    });

    final InstallSnapshotReplyProto reply = notifyInstallSnapshot();
    Assertions.assertEquals(InstallSnapshotResult.ALREADY_INSTALLED, reply.getResult());
    Assertions.assertEquals(snapshotIndex, reply.getSnapshotIndex());
    Assertions.assertEquals(RaftLog.INVALID_LOG_INDEX, handler.getInProgressInstallSnapshotIndex());
    Mockito.verifyNoInteractions(followerEvent);
    Assertions.assertFalse(pendingRead.isDone());
    readRequests.getAppliedIndexConsumer().accept(APPLIED_INDEX + 1);
    Assertions.assertEquals(APPLIED_INDEX + 1, pendingRead.join().longValue());
  }

  @ParameterizedTest
  @ValueSource(longs = {-1, 8})
  public void testRequiredInstallationStillRejectsReadsAndCompletes(long snapshotIndex) throws Exception {
    Mockito.when(log.getSnapshotIndex()).thenReturn(snapshotIndex);
    final CompletableFuture<Long> pendingRead = readRequests.waitToAdvance(APPLIED_INDEX + 1, () -> null);

    Assertions.assertEquals(InstallSnapshotResult.IN_PROGRESS, notifyInstallSnapshot().getResult());
    Assertions.assertEquals(FIRST_AVAILABLE_INDEX, handler.getInProgressInstallSnapshotIndex());
    Assertions.assertInstanceOf(ReadException.class,
        Assertions.assertThrows(CompletionException.class, pendingRead::join).getCause());
    Assertions.assertInstanceOf(ReadException.class,
        Assertions.assertThrows(CompletionException.class, () -> readOnlyAsync().join()).getCause());

    // Snapshot files can cover the boundary before the notification has reloaded the state machine.
    final TermIndex installed = TermIndex.valueOf(TERM, 20);
    Mockito.when(log.getSnapshotIndex()).thenReturn(installed.getIndex());
    Assertions.assertEquals(InstallSnapshotResult.IN_PROGRESS, notifyInstallSnapshot().getResult());
    Assertions.assertEquals(FIRST_AVAILABLE_INDEX, handler.getInProgressInstallSnapshotIndex());
    snapshotFuture.complete(installed);

    final InstallSnapshotReplyProto reply = notifyInstallSnapshot();
    Assertions.assertEquals(InstallSnapshotResult.SNAPSHOT_INSTALLED, reply.getResult());
    Assertions.assertEquals(installed.getIndex(), reply.getSnapshotIndex());
    Mockito.verify(stateMachine).pause();
    Mockito.verify(state).reloadStateMachine(installed);
    Mockito.verify(followerEvent).notifyInstallSnapshotFromLeader(Mockito.any(), Mockito.any());
    Assertions.assertEquals(RaftLog.INVALID_LOG_INDEX, handler.getInProgressInstallSnapshotIndex());
    Assertions.assertSame(query, readOnlyAsync().join());
  }
}
