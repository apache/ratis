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
import org.apache.ratis.proto.RaftProtos.ReadIndexReplyProto;
import org.apache.ratis.proto.RaftProtos.ReadIndexRequestProto;
import org.apache.ratis.proto.RaftProtos.ReadRequestTypeProto;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.LeaderNotReadyException;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.protocol.exceptions.ReadException;
import org.apache.ratis.protocol.exceptions.ReadIndexException;
import org.apache.ratis.protocol.exceptions.ServerNotReadyException;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.protocol.RaftServerAsynchronousProtocol;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.LifeCycle;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestReadIndex {
  @ParameterizedTest
  @CsvSource({"DEFAULT, false", "LINEARIZABLE, false", "LINEARIZABLE, true"})
  public void testLocalReadOnlyUsesCustomExecutor(RaftServerConfigKeys.Read.Option option,
      boolean preferNonLinearizable) throws Exception {
    final Fixture fixture = new Fixture(option);
    when(fixture.server.getInfo().isLeader()).thenReturn(true);
    when(fixture.server.getInfo().isLeaderReady()).thenReturn(true);
    fixture.reads.getAppliedIndexConsumer().accept(10);
    fixture.reply.complete(fixture.successReply(10));
    final Message query = Message.valueOf("query");
    final Message answer = Message.valueOf("answer");
    final CompletableFuture<Message> result = new CompletableFuture<>();
    when(fixture.stateMachine.query(query)).thenReturn(result);
    final Queue<Runnable> queries = new ArrayDeque<>();

    final CompletableFuture<Message> read = fixture.handler.readOnlyAsync(fixture.clientId,
        RaftClientRequest.readRequestType(preferNonLinearizable).getRead(), query, queries::add);
    verify(fixture.stateMachine, never()).query(any());
    Assertions.assertTrue(fixture.queries.isEmpty());
    Assertions.assertEquals(1, queries.size());
    queries.remove().run();
    Assertions.assertFalse(read.isDone());
    result.complete(answer);
    Assertions.assertSame(answer, read.get(5, TimeUnit.SECONDS));
  }

  @Test
  public void testLocalReadOnlyUsesServerExecutorAfterApply() throws Exception {
    final Fixture fixture = new Fixture(RaftServerConfigKeys.Read.Option.LINEARIZABLE);
    final Message query = Message.valueOf("query");
    final Message answer = Message.valueOf("answer");
    when(fixture.stateMachine.query(query)).thenReturn(CompletableFuture.completedFuture(answer));
    final CompletableFuture<Message> first = fixture.handler.readOnlyAsync(fixture.clientId, fixture.readType, query, null);
    final CompletableFuture<Message> second = fixture.handler.readOnlyAsync(fixture.clientId, fixture.readType, query, null);
    fixture.reply.complete(fixture.successReply(10));
    Assertions.assertTrue(fixture.queries.isEmpty());
    fixture.reads.getAppliedIndexConsumer().accept(10);
    verify(fixture.stateMachine, never()).query(any());
    Assertions.assertEquals(2, fixture.queries.size());
    fixture.queries.remove().run();
    Assertions.assertNotEquals(first.isDone(), second.isDone());
    fixture.queries.remove().run();
    Assertions.assertSame(answer, first.get(5, TimeUnit.SECONDS));
    Assertions.assertSame(answer, second.get(5, TimeUnit.SECONDS));
  }

  @ParameterizedTest
  @CsvSource({"DEFAULT, false", "LINEARIZABLE, true"})
  public void testLocalReadOnlyUsesServerExecutorWithoutBarrier(RaftServerConfigKeys.Read.Option option,
      boolean preferNonLinearizable) throws Exception {
    final Fixture fixture = new Fixture(option);
    when(fixture.server.getInfo().isLeader()).thenReturn(true);
    when(fixture.server.getInfo().isLeaderReady()).thenReturn(true);
    final Message query = Message.valueOf("query");
    final Message answer = Message.valueOf("answer");
    when(fixture.stateMachine.query(query)).thenReturn(CompletableFuture.completedFuture(answer));
    final CompletableFuture<Message> read = fixture.handler.readOnlyAsync(fixture.clientId,
        RaftClientRequest.readRequestType(preferNonLinearizable).getRead(), query, null);
    Assertions.assertFalse(read.isDone());
    verify(fixture.stateMachine, never()).query(any());
    verify(fixture.rpc, never()).readIndexAsync(any());
    Assertions.assertEquals(1, fixture.queries.size());
    fixture.queries.remove().run();
    Assertions.assertSame(answer, read.get(5, TimeUnit.SECONDS));
  }

  @ParameterizedTest
  @EnumSource(RaftServerConfigKeys.Read.Option.class)
  public void testLocalReadOnlyExecutorRejection(RaftServerConfigKeys.Read.Option option) throws Exception {
    final Fixture fixture = new Fixture(option);
    when(fixture.server.getInfo().isLeader()).thenReturn(true);
    when(fixture.server.getInfo().isLeaderReady()).thenReturn(true);
    fixture.reads.getAppliedIndexConsumer().accept(10);
    fixture.reply.complete(fixture.successReply(10));
    final RejectedExecutionException failure = new RejectedExecutionException("query executor stopped");
    final Executor executor = task -> {
      throw failure;
    };
    final CompletableFuture<Message> read = fixture.handler.readOnlyAsync(fixture.clientId,
        fixture.readType, Message.valueOf("query"), executor);
    Assertions.assertSame(failure, Assertions.assertThrows(CompletionException.class, read::join).getCause());
    verify(fixture.stateMachine, never()).query(any());
    Assertions.assertEquals(10L, fixture.handler.readIndexAsync(fixture.clientId, fixture.readType).join().longValue());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testLocalReadOnlyQueryFailure(boolean synchronous) throws Exception {
    final Fixture fixture = new Fixture(RaftServerConfigKeys.Read.Option.LINEARIZABLE);
    fixture.reads.getAppliedIndexConsumer().accept(10);
    fixture.reply.complete(fixture.successReply(10));
    final Message query = Message.valueOf("query");
    final IllegalStateException failure = new IllegalStateException("query failed");
    if (synchronous) {
      when(fixture.stateMachine.query(query)).thenThrow(failure);
    } else {
      final CompletableFuture<Message> result = new CompletableFuture<>();
      result.completeExceptionally(failure);
      when(fixture.stateMachine.query(query)).thenReturn(result);
    }
    final CompletableFuture<Message> read = fixture.handler.readOnlyAsync(fixture.clientId,
        fixture.readType, query, null);
    fixture.queries.remove().run();
    Assertions.assertSame(failure, Assertions.assertThrows(CompletionException.class, read::join).getCause());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testLocalReadOnlyBarrierFailure(boolean afterConfirmation) throws Exception {
    final Fixture fixture = new Fixture(RaftServerConfigKeys.Read.Option.LINEARIZABLE);
    final CompletableFuture<Message> read = fixture.handler.readOnlyAsync(fixture.clientId,
        fixture.readType, Message.valueOf("query"), null);
    final ReadException failure = new ReadException("read failed");
    if (afterConfirmation) {
      fixture.reply.complete(fixture.successReply(10));
      fixture.reads.fail(failure);
    } else {
      fixture.reply.completeExceptionally(failure);
    }
    Assertions.assertSame(failure, Assertions.assertThrows(CompletionException.class, read::join).getCause());
    Assertions.assertTrue(fixture.queries.isEmpty());
    verify(fixture.stateMachine, never()).query(any());
  }

  @Test
  public void testBlockingLocalQueryDoesNotBlockReadIndexCompletion() throws Exception {
    final Fixture fixture = new Fixture(RaftServerConfigKeys.Read.Option.LINEARIZABLE);
    final Message query = Message.valueOf("query");
    final Message answer = Message.valueOf("answer");
    final CountDownLatch started = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    when(fixture.stateMachine.query(query)).thenAnswer(invocation -> {
      started.countDown();
      Assertions.assertTrue(release.await(5, TimeUnit.SECONDS));
      return CompletableFuture.completedFuture(answer);
    });
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      final CompletableFuture<Message> read = fixture.handler.readOnlyAsync(fixture.clientId,
          fixture.readType, query, executor);
      fixture.reply.complete(fixture.successReply(10));
      fixture.reads.getAppliedIndexConsumer().accept(10);
      Assertions.assertTrue(started.await(5, TimeUnit.SECONDS));
      Assertions.assertEquals(10L, fixture.handler.readIndexAsync(fixture.clientId, fixture.readType).join().longValue());
      Assertions.assertFalse(read.isDone());
      release.countDown();
      Assertions.assertSame(answer, read.get(5, TimeUnit.SECONDS));
      Assertions.assertFalse(executor.isShutdown(), "Ratis must not shut down a caller-owned executor");
    } finally {
      release.countDown();
      executor.shutdownNow();
      Assertions.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testLocalReadOnlyRequiresReadyLeaderWithoutBarrier(boolean leader) throws Exception {
    final Fixture fixture = new Fixture(RaftServerConfigKeys.Read.Option.DEFAULT);
    when(fixture.server.getInfo().isLeader()).thenReturn(leader);
    final CompletableFuture<Message> read = fixture.handler.readOnlyAsync(fixture.clientId,
        fixture.readType, Message.valueOf("query"), null);
    final Throwable failure = Assertions.assertThrows(CompletionException.class, read::join).getCause();
    if (leader) {
      Assertions.assertInstanceOf(LeaderNotReadyException.class, failure);
    } else {
      Assertions.assertInstanceOf(NotLeaderException.class, failure);
    }
    Assertions.assertTrue(fixture.queries.isEmpty());
    verify(fixture.rpc, never()).readIndexAsync(any());
  }

  @Test
  public void testLocalReadOnlyRejectsInvalidArgumentsAndStoppedDivision() throws Exception {
    final Fixture fixture = new Fixture(RaftServerConfigKeys.Read.Option.DEFAULT);
    final Message query = Message.valueOf("query");
    Assertions.assertThrows(NullPointerException.class,
        () -> fixture.handler.readOnlyAsync(null, fixture.readType, query, null));
    Assertions.assertThrows(NullPointerException.class,
        () -> fixture.handler.readOnlyAsync(fixture.clientId, null, query, null));
    Assertions.assertThrows(NullPointerException.class,
        () -> fixture.handler.readOnlyAsync(fixture.clientId, fixture.readType, null, null));
    doThrow(new ServerNotReadyException("division is stopped")).when(fixture.server)
        .assertLifeCycleState(LifeCycle.States.RUNNING);
    Assertions.assertThrows(ServerNotReadyException.class,
        () -> fixture.handler.readOnlyAsync(fixture.clientId, fixture.readType, query, null));
  }

  @ParameterizedTest
  @EnumSource(RaftServerConfigKeys.Read.Option.class)
  public void testWaitsForLocalApplication(RaftServerConfigKeys.Read.Option option) throws Exception {
    final Fixture fixture = new Fixture(option);
    final CompletableFuture<Long> read = fixture.handler.readIndexAsync(fixture.clientId, fixture.readType);
    Assertions.assertFalse(read.isDone());

    fixture.reply.complete(fixture.successReply(10));
    Assertions.assertFalse(read.isDone());
    fixture.reads.getAppliedIndexConsumer().accept(9);
    Assertions.assertFalse(read.isDone());
    fixture.reads.getAppliedIndexConsumer().accept(12);
    Assertions.assertEquals(10L, read.join().longValue());
    verify(fixture.stateMachine, never()).query(any());
  }

  @Test
  public void testReturnsConfirmedIndexWhenAlreadyApplied() throws Exception {
    final Fixture fixture = new Fixture(RaftServerConfigKeys.Read.Option.DEFAULT);
    fixture.reads.getAppliedIndexConsumer().accept(20);
    fixture.reply.complete(fixture.successReply(10));
    Assertions.assertEquals(10L, fixture.handler.readIndexAsync(fixture.clientId, fixture.readType).join().longValue());
    verify(fixture.stateMachine, never()).query(any());
  }

  @Test
  public void testForwardsReadAfterWriteSemantics() throws Exception {
    final Fixture fixture = new Fixture(RaftServerConfigKeys.Read.Option.DEFAULT);
    final ReadRequestTypeProto type = ReadRequestTypeProto.newBuilder()
        .setReadAfterWriteConsistent(true).setGlobal(true).build();
    final CompletableFuture<Long> read = fixture.handler.readIndexAsync(fixture.clientId, type);
    final ArgumentCaptor<ReadIndexRequestProto> request = ArgumentCaptor.forClass(ReadIndexRequestProto.class);
    verify(fixture.rpc).readIndexAsync(request.capture());
    final RaftClientRequest clientRequest = ClientProtoUtils.toRaftClientRequest(request.getValue().getClientRequest());
    Assertions.assertEquals(fixture.clientId, clientRequest.getClientId());
    Assertions.assertEquals(type, clientRequest.getType().getRead());
    fixture.reads.getAppliedIndexConsumer().accept(10);
    fixture.reply.complete(fixture.successReply(10));
    Assertions.assertEquals(10L, read.join().longValue());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testLeaderWaitsForClientWrite(boolean global) throws Exception {
    final Fixture fixture = new Fixture(RaftServerConfigKeys.Read.Option.DEFAULT);
    final LeaderStateImpl leader = mock(LeaderStateImpl.class);
    when(fixture.server.getRole().getLeaderState()).thenReturn(Optional.of(leader));
    final WriteIndexCache cache = new WriteIndexCache(new RaftProperties());
    when(fixture.server.getWriteIndexCache()).thenReturn(cache);
    final CompletableFuture<Long> write = new CompletableFuture<>();
    cache.add(global ? ClientId.randomId() : fixture.clientId, write);
    when(leader.getReadIndex(7L)).thenReturn(CompletableFuture.completedFuture(10L));
    final ReadRequestTypeProto type = ReadRequestTypeProto.newBuilder()
        .setReadAfterWriteConsistent(true).setGlobal(global).build();
    final CompletableFuture<Long> read = fixture.handler.readIndexAsync(fixture.clientId, type);
    Assertions.assertFalse(read.isDone());
    verify(leader, never()).getReadIndex(any());
    write.complete(7L);
    Assertions.assertFalse(read.isDone());
    fixture.reads.getAppliedIndexConsumer().accept(10);
    Assertions.assertEquals(10L, read.join().longValue());
    verify(fixture.rpc, never()).readIndexAsync(any());
    verify(fixture.stateMachine, never()).query(any());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testCancellationDoesNotAffectOtherReaders(boolean beforeConfirmation) throws Exception {
    final Fixture fixture = new Fixture(RaftServerConfigKeys.Read.Option.DEFAULT);
    final CompletableFuture<Long> first = fixture.handler.readIndexAsync(fixture.clientId, fixture.readType);
    final CompletableFuture<Long> second = fixture.handler.readIndexAsync(fixture.clientId, fixture.readType);
    if (!beforeConfirmation) {
      fixture.reply.complete(fixture.successReply(10));
    }
    Assertions.assertTrue(first.cancel(false));
    Assertions.assertFalse(fixture.reply.isCancelled());
    fixture.reply.complete(fixture.successReply(10));
    Assertions.assertFalse(second.isDone());
    fixture.reads.getAppliedIndexConsumer().accept(10);
    Assertions.assertEquals(10L, second.join().longValue());
  }

  @Test
  public void testRpcFailure() throws Exception {
    final Fixture fixture = new Fixture(RaftServerConfigKeys.Read.Option.DEFAULT);
    final CompletableFuture<Long> read = fixture.handler.readIndexAsync(fixture.clientId, fixture.readType);
    final IOException failure = new IOException("read index RPC failed");
    fixture.reply.completeExceptionally(failure);
    Assertions.assertSame(failure, Assertions.assertThrows(CompletionException.class, read::join).getCause());
    verify(fixture.stateMachine, never()).query(any());
  }

  @Test
  public void testUnsuccessfulReply() throws Exception {
    final Fixture fixture = new Fixture(RaftServerConfigKeys.Read.Option.DEFAULT);
    final CompletableFuture<Long> read = fixture.handler.readIndexAsync(fixture.clientId, fixture.readType);
    fixture.reply.complete(ReadIndexReplyProto.getDefaultInstance());
    Assertions.assertInstanceOf(ReadIndexException.class,
        Assertions.assertThrows(CompletionException.class, read::join).getCause());
  }

  @Test
  public void testApplyFailure() throws Exception {
    final Fixture fixture = new Fixture(RaftServerConfigKeys.Read.Option.DEFAULT);
    final CompletableFuture<Long> read = fixture.handler.readIndexAsync(fixture.clientId, fixture.readType);
    fixture.reply.complete(fixture.successReply(10));
    final ReadException failure = new ReadException("snapshot installation started");
    fixture.reads.fail(failure);
    Assertions.assertSame(failure, Assertions.assertThrows(CompletionException.class, read::join).getCause());
    verify(fixture.stateMachine, never()).query(any());
  }

  @Test
  public void testRejectsInvalidArguments() throws Exception {
    final Fixture fixture = new Fixture(RaftServerConfigKeys.Read.Option.DEFAULT);
    Assertions.assertThrows(NullPointerException.class, () -> fixture.handler.readIndexAsync(null, fixture.readType));
    Assertions.assertThrows(NullPointerException.class, () -> fixture.handler.readIndexAsync(fixture.clientId, null));
    Assertions.assertThrows(IllegalArgumentException.class, () -> fixture.handler.readIndexAsync(fixture.clientId,
        RaftClientRequest.readRequestType(true).getRead()));
    verify(fixture.rpc, never()).readIndexAsync(any());
  }

  @Test
  public void testRejectsStoppedDivision() throws Exception {
    final Fixture fixture = new Fixture(RaftServerConfigKeys.Read.Option.DEFAULT);
    final ServerNotReadyException failure = new ServerNotReadyException("division is stopped");
    doThrow(failure).when(fixture.server).assertLifeCycleState(LifeCycle.States.RUNNING);
    Assertions.assertSame(failure, Assertions.assertThrows(ServerNotReadyException.class,
        () -> fixture.handler.readIndexAsync(fixture.clientId, fixture.readType)));
    verify(fixture.rpc, never()).readIndexAsync(any());
  }

  private static class Fixture {
    private final ClientId clientId = ClientId.randomId();
    private final ReadRequestTypeProto readType = RaftClientRequest.readRequestType().getRead();
    private final RaftPeerId leaderId = RaftPeerId.valueOf("leader");
    private final RaftGroupMemberId memberId = RaftGroupMemberId.valueOf(
        RaftPeerId.valueOf("follower"), RaftGroupId.randomId());
    private final RaftServerImpl server = mock(RaftServerImpl.class, RETURNS_DEEP_STUBS);
    private final StateMachine stateMachine = mock(StateMachine.class);
    private final RaftServerAsynchronousProtocol rpc = mock(RaftServerAsynchronousProtocol.class);
    private final CompletableFuture<ReadIndexReplyProto> reply = new CompletableFuture<>();
    private final Queue<Runnable> queries = new ArrayDeque<>();
    private final ReadRequests reads;
    private final ReadRequestHandler handler;

    Fixture(RaftServerConfigKeys.Read.Option option) throws IOException {
      final RaftProperties properties = new RaftProperties();
      RaftServerConfigKeys.Read.setOption(properties, option);
      reads = new ReadRequests(0, properties);
      when(server.getId()).thenReturn(memberId.getPeerId());
      when(server.getState().getMemberId()).thenReturn(memberId);
      when(server.getState().getReadRequests()).thenReturn(reads);
      when(server.getStateMachine()).thenReturn(stateMachine);
      when(server.getRole().getLeaderState()).thenReturn(Optional.empty());
      when(server.getInfo().getLeaderId()).thenReturn(leaderId);
      when(server.getSnapshotInstallationHandler().getInProgressInstallSnapshotIndex())
          .thenReturn(RaftLog.INVALID_LOG_INDEX);
      when(server.getServerRpc().async()).thenReturn(rpc);
      when(rpc.readIndexAsync(any())).thenReturn(reply);
      final ExecutorService serverExecutor = server.getServerExecutor();
      doAnswer(invocation -> queries.add(invocation.getArgument(0))).when(serverExecutor).execute(any());
      handler = new ReadRequestHandler(server, properties);
    }

    ReadIndexReplyProto successReply(long index) {
      return ServerProtoUtils.toReadIndexReplyProto(memberId.getPeerId(),
          RaftGroupMemberId.valueOf(leaderId, memberId.getGroupId()), true, index);
    }
  }
}
