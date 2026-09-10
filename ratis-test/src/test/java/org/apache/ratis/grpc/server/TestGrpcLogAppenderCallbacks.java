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

import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.grpc.GrpcLogAppenderListener;
import org.apache.ratis.grpc.metrics.GrpcServerMetrics;
import org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto.AppendResult;
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.RaftRpcReplyProto;
import org.apache.ratis.proto.RaftProtos.RaftRpcRequestProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.leader.FollowerInfo;
import org.apache.ratis.server.leader.LeaderState;
import org.apache.ratis.server.leader.LogAppender;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.apache.ratis.util.AutoCloseableLock;
import org.apache.ratis.util.AutoCloseableReadWriteLock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@Timeout(value = 10, unit = TimeUnit.SECONDS)
public class TestGrpcLogAppenderCallbacks {
  private final RaftPeerId source = RaftPeerId.valueOf("source");
  private final RaftPeerId destination = RaftPeerId.valueOf("destination");
  private final GrpcLogAppenderListener listener = mock(GrpcLogAppenderListener.class);
  private final GrpcLogAppenderListener.AppendEntries appendEntries = mock(GrpcLogAppenderListener.AppendEntries.class);
  private final RaftServer.Division server = mock(RaftServer.Division.class, RETURNS_DEEP_STUBS);
  private final FollowerInfo follower = mock(FollowerInfo.class, RETURNS_DEEP_STUBS);
  private final LeaderState leaderState = mock(LeaderState.class);
  private final GrpcServicesImpl serverRpc = mock(GrpcServicesImpl.class, RETURNS_DEEP_STUBS);
  private final GrpcServerProtocolClient client = mock(GrpcServerProtocolClient.class);
  private GrpcLogAppender appender;
  private GrpcLogAppender.RequestMap pending;
  private GrpcServerMetrics metrics;
  private StreamObserver<AppendEntriesReplyProto> responses;

  @BeforeEach
  public void setup() throws Exception {
    when(server.getRaftServer().getProperties()).thenReturn(new RaftProperties());
    when(server.getRaftServer().getServerRpc()).thenReturn(serverRpc);
    when(server.getId()).thenReturn(source);
    when(server.getMemberId()).thenReturn(RaftGroupMemberId.valueOf(source, RaftGroupId.randomId()));
    when(server.getInfo().isAlive()).thenReturn(true);
    when(server.getInfo().isLeader()).thenReturn(true);
    when(server.getRaftLog().isOpened()).thenReturn(true);
    when(follower.getId()).thenReturn(destination);
    when(follower.getName()).thenReturn("test-follower");
    when(serverRpc.getProxies().getProxy(destination)).thenReturn(client);
    when(listener.appendEntries()).thenReturn(appendEntries);
    appender = spy(new GrpcLogAppender(server, leaderState, follower, listener));
    pending = (GrpcLogAppender.RequestMap) RaftTestUtil.getDeclaredField(appender, "pendingRequests");
    metrics = (GrpcServerMetrics) RaftTestUtil.getDeclaredField(appender, "grpcServerMetrics");
    responses = appender.new AppendLogResponseHandler();
    Assertions.assertTrue(appender.isRunning());
  }

  @AfterEach
  public void cleanup() throws Exception {
    if (appender != null) {
      appender.stopAsync().get(5, TimeUnit.SECONDS);
    }
  }

  private AppendEntriesRequestProto request(long callId) {
    return AppendEntriesRequestProto.newBuilder()
        .setServerRequest(RaftRpcRequestProto.newBuilder().setCallId(callId))
        .addEntries(LogEntryProto.newBuilder().setIndex(callId)
            .setStateMachineLogEntry(StateMachineLogEntryProto.getDefaultInstance()))
        .build();
  }

  private void addPending(long callId) {
    final GrpcLogAppender.AppendEntriesRequest request =
        new GrpcLogAppender.AppendEntriesRequest(request(callId), destination, metrics);
    request.startRequestTimer();
    pending.put(request);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testStreamErrorAfterStopOrLeaderChange(boolean stop) throws Exception {
    addPending(1);
    addPending(2);
    if (stop) {
      appender.stopAsync().get(5, TimeUnit.SECONDS);
    } else {
      when(server.getInfo().isLeader()).thenReturn(false);
    }
    clearInvocations(client, leaderState, follower, follower.getErrorState());
    final IOException error = new IOException("Stream failed after leadership ended");
    doThrow(new IllegalStateException("Listener failure")).when(appendEntries).onError(error);
    Assertions.assertDoesNotThrow(() -> responses.onError(error));
    verify(appendEntries).onError(error);
    verifyNoInteractions(client, leaderState, follower.getErrorState());
    verify(follower, never()).computeNextIndex(any());
  }

  @Test
  public void testResetNotificationPrecedesClientFailure() throws Exception {
    addPending(1);
    when(serverRpc.getProxies().getProxy(destination)).thenThrow(new IOException("Closed client"));
    final IOException error = new IOException("Original stream error");
    doThrow(new IllegalStateException("Listener failure")).when(listener).onResetClient(anyString(), eq(error));
    responses.onError(error);
    final InOrder order = inOrder(appendEntries, listener);
    order.verify(appendEntries).onError(error);
    order.verify(listener).onResetClient(anyString(), eq(error));
  }

  @Test
  public void testCompletionAfterStop() throws Exception {
    addPending(1);
    appender.stopAsync().get(5, TimeUnit.SECONDS);
    clearInvocations(client, leaderState);
    responses.onCompleted();
    verify(appendEntries).onCompleted();
    verify(listener, never()).onResetClient(anyString(), any());
    verifyNoInteractions(client, leaderState);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testStreamCreationFailure(boolean failProxy) throws Exception {
    final AppendEntriesRequestProto request = request(1);
    doReturn(request).when(appender).newAppendEntriesRequest(0, false);
    final Exception error;
    if (failProxy) {
      error = new IOException("Failed to create peer client");
      when(serverRpc.getProxies().getProxy(destination)).thenThrow(error);
    } else {
      error = new IllegalStateException("Failed to create append stream");
      when(client.appendEntries(any(), eq(false))).thenThrow(error);
    }
    Assertions.assertSame(error, Assertions.assertThrows(Exception.class, () -> appender.appendLog(false)));
    final InOrder order = inOrder(appendEntries);
    order.verify(appendEntries).onRequest(request);
    order.verify(appendEntries).onFailure(1, error);
    Assertions.assertFalse(appender.hasPendingDataRequests());
  }

  @Test
  public void testTimeout() {
    addPending(1);
    appender.timeoutAppendRequest(1, false);
    appender.timeoutAppendRequest(1, false);
    verify(appendEntries).onTimeout(1);
  }

  @ParameterizedTest
  @EnumSource(value = AppendResult.class, names = {"SUCCESS", "NOT_LEADER", "INCONSISTENCY"})
  public void testReplyAndInvalidation(AppendResult result) {
    addPending(1);
    addPending(2);
    final AppendEntriesReplyProto reply = AppendEntriesReplyProto.newBuilder()
        .setServerReply(RaftRpcReplyProto.newBuilder().setCallId(1)).setResult(result).build();
    responses.onNext(reply);
    responses.onNext(reply);
    verify(appendEntries, times(2)).onReply(reply);
    if (result == AppendResult.INCONSISTENCY) {
      final InOrder order = inOrder(appendEntries);
      order.verify(appendEntries).onReply(reply);
      order.verify(appendEntries).onReplyInconsistency();
      verify(appendEntries, times(2)).onReplyInconsistency();
      Assertions.assertFalse(appender.hasPendingDataRequests());
    } else {
      verify(appendEntries, never()).onReplyInconsistency();
    }
    verify(listener, never()).onResetClient(anyString(), any());
    appender.timeoutAppendRequest(1, false);
    verify(appendEntries, never()).onTimeout(1);
  }

  @Test
  public void testReplyDoesNotAcquireAppenderWriteLock() throws Exception {
    addPending(1);
    final AppendEntriesReplyProto reply = AppendEntriesReplyProto.newBuilder()
        .setServerReply(RaftRpcReplyProto.newBuilder().setCallId(1)).setResult(AppendResult.SUCCESS).build();
    final AutoCloseableReadWriteLock appenderLock =
        (AutoCloseableReadWriteLock) RaftTestUtil.getDeclaredField(appender, "lock");
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    try (AutoCloseableLock ignored = appenderLock.writeLock(null, null)) {
      CompletableFuture.runAsync(() -> responses.onNext(reply), executor).get(5, TimeUnit.SECONDS);
      verify(appendEntries).onReply(reply);
      Assertions.assertFalse(appender.hasPendingDataRequests());
    } finally {
      executor.shutdownNow();
      Assertions.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testListenerInitializationFailureIsIsolated(boolean failFactory) throws Exception {
    when(listener.appendEntries()).thenThrow(new IllegalStateException("Injected accessor failure"));
    final Parameters parameters = new Parameters();
    GrpcConfigKeys.Server.setLogAppenderListenerFactory(parameters, (member, peer) -> {
      Assertions.assertEquals(server.getMemberId(), member);
      Assertions.assertEquals(follower.getPeer(), peer);
      if (failFactory) {
        throw new IllegalStateException("Injected factory failure");
      }
      return listener;
    });
    final LogAppender created = new GrpcFactory(parameters).newLogAppender(server, leaderState, follower);
    Assertions.assertNotNull(created);
    created.stopAsync().get(5, TimeUnit.SECONDS);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testRunLoopExit(boolean exceptional) throws Exception {
    addPending(1);
    doThrow(new IllegalStateException("Listener failure")).when(listener).onNotRunning();
    if (exceptional) {
      final InterruptedIOException error = new InterruptedIOException("Interrupted send");
      doThrow(error).when(appender).appendLog(true);
      Assertions.assertSame(error, Assertions.assertThrows(IOException.class, appender::run));
    } else {
      when(server.getInfo().isLeader()).thenReturn(false);
      appender.run();
    }
    verify(listener).onNotRunning();
  }
}
