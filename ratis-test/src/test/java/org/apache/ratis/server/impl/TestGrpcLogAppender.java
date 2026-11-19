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

import org.apache.ratis.BaseTest;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.server.GrpcLogAppender;
import org.apache.ratis.grpc.server.GrpcServicesImpl;
import org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.RaftRpcRequestProto;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.leader.LeaderState;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.apache.ratis.util.Timestamp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

class TestGrpcLogAppender extends BaseTest {
  @Test
  void testRequestUsesOneNextIndex() throws Exception {
    try (Appender appender = new Appender()) {
      when(appender.log.getTermIndex(1)).thenAnswer(invocation -> {
        appender.follower.updateNextIndex(6);
        return TermIndex.valueOf(1, 1);
      });

      final AppendEntriesRequestProto request = appender.appender.newAppendEntriesRequest(0, false);
      Assertions.assertNotNull(request);
      Assertions.assertEquals(1, request.getPreviousLog().getIndex());
      Assertions.assertEquals(2, request.getEntries(0).getIndex());
      Assertions.assertEquals(6, appender.follower.getNextIndex());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testConcurrentAppendReplyDoesNotRegressNextIndex(boolean afterConstruction) throws Exception {
    try (Appender appender = new Appender()) {
      final CountDownLatch building = new CountDownLatch(1);
      final CountDownLatch proceed = new CountDownLatch(1);
      if (afterConstruction) {
        doAnswer(invocation -> {
          final AppendEntriesRequestProto request = (AppendEntriesRequestProto) invocation.callRealMethod();
          building.countDown();
          Assertions.assertTrue(proceed.await(5, TimeUnit.SECONDS));
          return request;
        }).when(appender.appender).newAppendEntriesRequest(anyLong(), anyBoolean());
      } else {
        when(appender.log.getTermIndex(1)).thenAnswer(invocation -> {
          building.countDown();
          Assertions.assertTrue(proceed.await(5, TimeUnit.SECONDS));
          return TermIndex.valueOf(1, 1);
        });
      }

      final Runnable receiveReply = appender.newAppendReply();
      final ExecutorService executor = Executors.newFixedThreadPool(2);
      try {
        final Future<?> append = executor.submit(() -> {
          appender.appendLog.invoke(appender.appender, false);
          return null;
        });
        Assertions.assertTrue(building.await(5, TimeUnit.SECONDS));
        final Future<?> response = executor.submit(receiveReply);
        response.get(3, TimeUnit.SECONDS);
        Assertions.assertEquals(6, appender.follower.getNextIndex());
        Assertions.assertFalse(append.isDone(), "Request preparation must still be paused");

        proceed.countDown();
        append.get(5, TimeUnit.SECONDS);
        response.get(5, TimeUnit.SECONDS);
        Assertions.assertEquals(6, appender.follower.getNextIndex());
        Assertions.assertEquals(5, appender.follower.getMatchIndex());
        Assertions.assertEquals(1, appender.getPendingRequestCount());
      } finally {
        proceed.countDown();
        executor.shutdown();
        Assertions.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
      }
    }
  }

  @Test
  void testInconsistencyRecomputesNextIndexAfterConcurrentAppendReply() throws Exception {
    try (Appender appender = new Appender()) {
      appender.follower.updateMatchIndex(1);
      final Runnable receiveSuccess = appender.newAppendReply();
      final Runnable receiveInconsistency = appender.newAppendReply(AppendEntriesReplyProto.newBuilder()
          .setResult(AppendEntriesReplyProto.AppendResult.INCONSISTENCY).setNextIndex(1).build());
      final CountDownLatch matchIndexRead = new CountDownLatch(1);
      final CountDownLatch proceed = new CountDownLatch(1);
      final AtomicBoolean first = new AtomicBoolean(true);
      doAnswer(invocation -> {
        final long matchIndex = (long) invocation.callRealMethod();
        if (first.compareAndSet(true, false)) {
          matchIndexRead.countDown();
          Assertions.assertTrue(proceed.await(5, TimeUnit.SECONDS));
        }
        return matchIndex;
      }).when(appender.follower).getMatchIndex();

      final ExecutorService executor = Executors.newFixedThreadPool(2);
      try {
        final Future<?> inconsistency = executor.submit(receiveInconsistency);
        Assertions.assertTrue(matchIndexRead.await(5, TimeUnit.SECONDS));
        // Advance both indexes after the inconsistency handler reads the old matchIndex.
        executor.submit(receiveSuccess).get(5, TimeUnit.SECONDS);
        Assertions.assertEquals(6, appender.follower.getNextIndex());

        proceed.countDown();
        inconsistency.get(5, TimeUnit.SECONDS);
        Assertions.assertEquals(5, appender.follower.getMatchIndex());
        Assertions.assertEquals(6, appender.follower.getNextIndex());
      } finally {
        proceed.countDown();
        executor.shutdown();
        Assertions.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
      }
    }
  }

  private static class Appender implements AutoCloseable {
    private final RaftLog log = mock(RaftLog.class);
    private final FollowerInfoImpl follower;
    private final GrpcLogAppender appender;
    private final Method appendLog;

    Appender() throws Exception {
      final RaftProperties properties = new RaftProperties();
      RaftServerConfigKeys.Log.Appender.setBufferElementLimit(properties, 1);
      final RaftGroupMemberId memberId = RaftGroupMemberId.valueOf(RaftPeerId.valueOf("leader"), RaftGroupId.randomId());
      final RaftPeer peer = RaftPeer.newBuilder().setId("follower").build();
      follower = spy(new FollowerInfoImpl(memberId, peer, id -> peer, Timestamp.currentTime(), 2, true));
      final RaftServer raftServer = mock(RaftServer.class);
      when(raftServer.getProperties()).thenReturn(properties);
      final RaftServer.Division server = mock(RaftServer.Division.class);
      when(server.getRaftServer()).thenReturn(raftServer);
      when(server.getMemberId()).thenReturn(memberId);
      when(server.getRaftLog()).thenReturn(log);
      when(raftServer.getServerRpc()).thenReturn(mock(GrpcServicesImpl.class));
      final LeaderState leader = mock(LeaderState.class);
      when(leader.newAppendEntriesRequestProto(eq(follower), anyList(), any(), anyLong())).thenAnswer(invocation -> {
        final List<LogEntryProto> entries = invocation.getArgument(1);
        final TermIndex previous = invocation.getArgument(2);
        return AppendEntriesRequestProto.newBuilder().addAllEntries(entries).setPreviousLog(previous.toProto())
            .setServerRequest(RaftRpcRequestProto.newBuilder().setCallId(invocation.getArgument(3))).build();
      });
      when(log.getNextIndex()).thenReturn(10L);
      when(log.getTermIndex(1)).thenReturn(TermIndex.valueOf(1, 1));
      when(log.getEntryWithData(anyLong())).thenAnswer(invocation -> {
        final long index = invocation.getArgument(0);
        final RaftLog.EntryWithData entry = mock(RaftLog.EntryWithData.class);
        when(entry.getEntry(any())).thenReturn(LogEntryProto.newBuilder().setTerm(1).setIndex(index).build());
        when(entry.getSerializedSize()).thenReturn(1);
        return entry;
      });
      appender = spy(new GrpcLogAppender(server, leader, follower, null));
      doReturn(Long.MAX_VALUE).when(appender).getHeartbeatWaitTimeMs();
      doReturn(false).when(appender).isRunning();
      final Field observers = GrpcLogAppender.class.getDeclaredField("appendLogRequestObserver");
      observers.setAccessible(true);
      observers.set(appender, mock(Class.forName(GrpcLogAppender.class.getName() + "$StreamObservers")));
      appendLog = GrpcLogAppender.class.getDeclaredMethod("appendLog", boolean.class);
      appendLog.setAccessible(true);
    }

    int getPendingRequestCount() throws Exception {
      final Field field = GrpcLogAppender.class.getDeclaredField("pendingRequests");
      field.setAccessible(true);
      final Map<?, ?> requests = (Map<?, ?>) RaftTestUtil.getDeclaredField(field.get(appender), "logRequests");
      return requests.size();
    }

    Runnable newAppendReply() throws Exception {
      return newAppendReply(AppendEntriesReplyProto.newBuilder()
          .setResult(AppendEntriesReplyProto.AppendResult.SUCCESS).setMatchIndex(5).setNextIndex(6).build());
    }

    @SuppressWarnings("unchecked")
    Runnable newAppendReply(AppendEntriesReplyProto reply) throws Exception {
      final Constructor<?> constructor = Class.forName(GrpcLogAppender.class.getName() + "$AppendLogResponseHandler")
          .getDeclaredConstructor(GrpcLogAppender.class);
      constructor.setAccessible(true);
      final StreamObserver<AppendEntriesReplyProto> handler =
          (StreamObserver<AppendEntriesReplyProto>) constructor.newInstance(appender);
      return () -> handler.onNext(reply);
    }

    @Override
    public void close() {
      appender.stopAsync();
    }
  }
}
