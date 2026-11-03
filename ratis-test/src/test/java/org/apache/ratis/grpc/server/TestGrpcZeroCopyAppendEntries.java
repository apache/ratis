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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import org.apache.ratis.BaseTest;
import org.apache.ratis.grpc.util.ZeroCopyMessageMarshaller;
import org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.RaftGroupIdProto;
import org.apache.ratis.proto.RaftProtos.RaftRpcRequestProto;
import org.apache.ratis.proto.RaftProtos.StateMachineEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.proto.grpc.RaftServerProtocolServiceGrpc;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.io.grpc.ServerMethodDefinition;
import org.apache.ratis.thirdparty.io.grpc.ServerServiceDefinition;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/** Zero-copy appendEntries in {@link GrpcServerProtocolService}: what is copied, and when buffers are released. */
public class TestGrpcZeroCopyAppendEntries extends BaseTest {
  private final RaftServer server = mock(RaftServer.class);
  @SuppressWarnings("unchecked")
  private final ZeroCopyMessageMarshaller<AppendEntriesRequestProto> marshaller = mock(ZeroCopyMessageMarshaller.class);
  @SuppressWarnings("unchecked")
  private final StreamObserver<AppendEntriesReplyProto> replies = mock(StreamObserver.class);

  private AppendEntriesRequestProto request;

  @BeforeEach
  public void setUp() {
    request = newRequest();
  }

  /** A request with one entry carrying state machine data and one without. */
  private static AppendEntriesRequestProto newRequest() {
    final byte[] data = new byte[64 << 10];
    new Random(1).nextBytes(data);
    final RaftRpcRequestProto serverRequest = RaftRpcRequestProto.newBuilder()
        .setRequestorId(ByteString.copyFromUtf8("s0"))
        .setReplyId(ByteString.copyFromUtf8("s1"))
        .setRaftGroupId(RaftGroupIdProto.newBuilder().setId(RaftGroupId.randomId().toByteString()))
        .setCallId(7)
        .build();
    final LogEntryProto withData = LogEntryProto.newBuilder().setTerm(1).setIndex(1)
        .setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder()
            .setLogData(ByteString.copyFromUtf8("write"))
            .setStateMachineEntry(StateMachineEntryProto.newBuilder().setStateMachineData(ByteString.copyFrom(data))))
        .build();
    final LogEntryProto withoutData = LogEntryProto.newBuilder().setTerm(1).setIndex(2)
        .setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(ByteString.copyFromUtf8("other")))
        .build();
    return AppendEntriesRequestProto.newBuilder().setServerRequest(serverRequest).setLeaderTerm(1)
        .addEntries(withData).addEntries(withoutData).build();
  }

  private static ByteString getStateMachineData(AppendEntriesRequestProto r) {
    return r.getEntries(0).getStateMachineLogEntry().getStateMachineEntry().getStateMachineData();
  }

  private StreamObserver<AppendEntriesRequestProto> newObserver(
      ZeroCopyMessageMarshaller<AppendEntriesRequestProto> zeroCopy) {
    final GrpcServerProtocolService service = new GrpcServerProtocolService(
        () -> RaftPeerId.valueOf("s1"), server, zeroCopy);
    return service.appendEntries(replies);
  }

  private AppendEntriesRequestProto captureSent() throws IOException {
    final ArgumentCaptor<AppendEntriesRequestProto> sent = ArgumentCaptor.forClass(AppendEntriesRequestProto.class);
    verify(server).appendEntriesAsync(sent.capture());
    return sent.getValue();
  }

  @Test
  public void testCopiesAllButStateMachineData() throws Exception {
    when(server.appendEntriesAsync(any())).thenReturn(new CompletableFuture<>());
    newObserver(marshaller).onNext(request);

    final AppendEntriesRequestProto sent = captureSent();
    assertNotSame(request, sent);
    assertEquals(request, sent);
    // only the state machine data still refers to the request's (zero-copy) buffers
    assertSame(getStateMachineData(request), getStateMachineData(sent));
    assertNotSame(request.getServerRequest().getRequestorId(), sent.getServerRequest().getRequestorId());
    assertNotSame(request.getEntries(0).getStateMachineLogEntry().getLogData(),
        sent.getEntries(0).getStateMachineLogEntry().getLogData());
    assertNotSame(request.getEntries(1), sent.getEntries(1));
  }

  @Test
  public void testReleasesAfterSuccessfulReply() throws Exception {
    final CompletableFuture<AppendEntriesReplyProto> reply = new CompletableFuture<>();
    when(server.appendEntriesAsync(any())).thenReturn(reply);
    newObserver(marshaller).onNext(request);
    // not before the follower has replied, i.e. written the entries and their state machine data
    verify(marshaller, never()).release(any());

    reply.complete(AppendEntriesReplyProto.getDefaultInstance());
    verify(marshaller).release(same(request));
  }

  @Test
  public void testReleasesWhenAppendFailsImmediately() throws Exception {
    when(server.appendEntriesAsync(any())).thenThrow(new IOException("test"));
    newObserver(marshaller).onNext(request);
    // nothing was appended, so nothing can still be using the buffers
    verify(marshaller).release(same(request));
  }

  @Test
  public void testKeepsBuffersWhenReplyFails() throws Exception {
    final CompletableFuture<AppendEntriesReplyProto> reply = new CompletableFuture<>();
    when(server.appendEntriesAsync(any())).thenReturn(reply);
    newObserver(marshaller).onNext(request);

    reply.completeExceptionally(new IOException("test"));
    // the log or the state machine may still be writing the data
    verify(marshaller, never()).release(any());
  }

  @Test
  public void testWithoutZeroCopy() throws Exception {
    when(server.appendEntriesAsync(any())).thenReturn(new CompletableFuture<>());
    newObserver(null).onNext(request);
    // passed through as is
    assertSame(request, captureSent());
  }

  @Test
  public void testBindServiceDefinition() {
    final String appendEntries = RaftServerProtocolServiceGrpc.getAppendEntriesMethod().getFullMethodName();
    final ServerServiceDefinition withZeroCopy = new GrpcServerProtocolService(
        () -> RaftPeerId.valueOf("s1"), server, true).bindServiceDefinition();
    for (ServerMethodDefinition<?, ?> method : withZeroCopy.getMethods()) {
      final String name = method.getMethodDescriptor().getFullMethodName();
      final Object requestMarshaller = method.getMethodDescriptor().getRequestMarshaller();
      if (name.equals(appendEntries)) {
        assertInstanceOf(ZeroCopyMessageMarshaller.class, requestMarshaller, name);
      } else {
        assertFalse(requestMarshaller instanceof ZeroCopyMessageMarshaller, name);
      }
    }

    final ServerServiceDefinition without = new GrpcServerProtocolService(
        () -> RaftPeerId.valueOf("s1"), server, false).bindServiceDefinition();
    for (ServerMethodDefinition<?, ?> method : without.getMethods()) {
      assertFalse(method.getMethodDescriptor().getRequestMarshaller() instanceof ZeroCopyMessageMarshaller,
          method.getMethodDescriptor().getFullMethodName());
    }
  }
}
