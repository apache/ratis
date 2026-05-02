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

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension;
import io.opentelemetry.sdk.trace.data.SpanData;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.RaftRpcRequestProto;
import org.apache.ratis.proto.RaftProtos.SpanContextProto;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.ServerNotReadyException;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachine4Testing;
import org.apache.ratis.trace.RatisAttributes;
import org.apache.ratis.trace.TraceConfigKeys;
import org.apache.ratis.trace.TraceServer;
import org.apache.ratis.trace.TraceUtils;
import org.apache.ratis.util.JavaUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;

public class RaftServerImplTracingTests {

  @RegisterExtension
  private static final OpenTelemetryExtension openTelemetryExtension =
      OpenTelemetryExtension.create();

  @Test
  public void testSubmitClientRequestAsync() throws Exception {
    final List<SpanData> spans = submitClientRequestAndCollectNewSpans(true);
    assertEquals(2, spans.size());
    assertTrue(
        spans.stream().anyMatch(s -> s.getKind() == SpanKind.CLIENT && s.getName().equals("client-span")),
        "Expected at least one span with SpanKind.CLIENT"
    );
    assertTrue(
        spans.stream().anyMatch(s -> s.getKind() == SpanKind.SERVER
            && s.getName().equals("raft.server.submitClientRequestAsync")),
        "Expected at least one span with SpanKind.SERVER"
    );

  }

  @Test
  public void testSubmitClientRequestAsyncTracingDisabled() throws Exception {
    final List<SpanData> spans = submitClientRequestAndCollectNewSpans(false);
    // Even when server-side tracing is disabled, we still emit the client span used to
    // generate the propagated context.
    assertEquals(1, spans.size());
    assertTrue(
        spans.stream().noneMatch(s -> s.getKind() == SpanKind.SERVER
            && s.getName().equals("raft.server.submitClientRequestAsync")),
        "Expected no SERVER span when tracing is disabled"
    );
    assertTrue(
        spans.stream().anyMatch(s -> s.getKind() == SpanKind.CLIENT && s.getName().equals("client-span")),
        "Expected at least one span with SpanKind.CLIENT"
    );
  }

  @Test
  public void testTraceAppendEntriesAsyncCreatesInternalSpan() throws Exception {
    long callId = randomCallId();
    int entriesCount = 3;
    final List<SpanData> spans = traceAppendEntriesAndCollectNewSpans(true, newAppendEntriesRequest(
        RaftPeerId.valueOf("leader1"), callId, entriesCount, injectedSpanContext()));
    final SpanData appendSpan = spans.stream()
        .filter(s -> s.getKind() == SpanKind.INTERNAL && s.getName().equals("raft.server.appendEntriesAsync"))
        .findFirst()
        .orElseThrow(() -> new IllegalStateException("Expected INTERNAL raft.server.appendEntriesAsync span"));
    assertEquals("n1", appendSpan.getAttributes().get(RatisAttributes.MEMBER_ID));
    assertEquals("leader1", appendSpan.getAttributes().get(RatisAttributes.PEER_ID));
    assertEquals(entriesCount, appendSpan.getAttributes().get(RatisAttributes.APPEND_ENTRIES_COUNT));
  }

  @Test
  public void testTraceAppendEntriesAsyncTracingDisabled() throws Exception {
    int entriesCount = 1;
    final List<SpanData> spans = traceAppendEntriesAndCollectNewSpans(false, newAppendEntriesRequest(
        RaftPeerId.valueOf("leader1"), randomCallId(), entriesCount, injectedSpanContext()));
    assertTrue(
        spans.stream().noneMatch(s -> s.getName().equals("raft.server.appendEntriesAsync")),
        "Expected no appendEntries span when tracing disabled, got: " + spans);
  }

  @Test
  public void testTraceAppendEntriesAsyncSkipsWhenSpanContextEmpty() throws Exception {
    int entriesCount = 1;
    final AppendEntriesRequestProto request = newAppendEntriesRequest(
        RaftPeerId.valueOf("leader1"), randomCallId(), entriesCount, SpanContextProto.getDefaultInstance());
    final List<SpanData> spans = traceAppendEntriesAndCollectNewSpans(true, request);
    assertEquals(1,
        spans.stream().filter(s -> s.getName().equals("raft.server.appendEntriesAsync")).count());
  }

  @Test
  public void testTraceAppendEntriesAsyncSpanRecordsErrorOnFailure() throws Exception {
    int entriesCount = 0;
    final List<SpanData> spans = traceAppendEntriesAndCollectNewSpans(true, newAppendEntriesRequest(
        RaftPeerId.valueOf("leader1"), randomCallId(), entriesCount, injectedSpanContext()),
        () -> JavaUtils.completeExceptionally(new IOException("Planned record error")));
    assertEquals(1,
        spans.stream().filter(s -> s.getName().equals("raft.server.appendEntriesAsync")).count());
    final SpanData appendSpan = spans.stream()
        .filter(s -> s.getKind() == SpanKind.INTERNAL && s.getName().equals("raft.server.appendEntriesAsync"))
        .findFirst()
        .orElseThrow(() -> new IllegalStateException("Expected INTERNAL raft.server.appendEntriesAsync span"));
    assertEquals(StatusCode.ERROR, appendSpan.getStatus().getStatusCode());
  }

  private static List<SpanData> traceAppendEntriesAndCollectNewSpans(
      boolean enableTracing, AppendEntriesRequestProto request) throws Exception {
    return traceAppendEntriesAndCollectNewSpans(enableTracing, request,
        () -> CompletableFuture.completedFuture(AppendEntriesReplyProto.getDefaultInstance()));
  }

  private static List<SpanData> traceAppendEntriesAndCollectNewSpans(
      boolean enableTracing,
      AppendEntriesRequestProto request,
      Supplier<CompletableFuture<AppendEntriesReplyProto>> action)
      throws Exception {
    final int before = openTelemetryExtension.getSpans().size();
    try {
      TraceUtils.setTracerWhenEnabled(enableTracing);
      final CompletableFuture<AppendEntriesReplyProto> traced =
          TraceServer.traceAppendEntriesAsync(action::get, request, "n1");
      try {
        traced.join();
      } catch (CompletionException e) {
        // allowed for failure-path test
      }
    } finally {
      TraceUtils.setTracerWhenEnabled(false);
    }
    final List<SpanData> after = openTelemetryExtension.getSpans();
    return new ArrayList<>(after.subList(before, after.size()));
  }

  private static SpanContextProto injectedSpanContext() {
    final Span remoteParent = openTelemetryExtension.getOpenTelemetry().getTracer("test")
        .spanBuilder("remote-parent")
        .setSpanKind(SpanKind.CLIENT)
        .startSpan();
    try {
      return TraceUtils.injectContextToProto(Context.current().with(remoteParent));
    } finally {
      remoteParent.end();
    }
  }

  private static AppendEntriesRequestProto newAppendEntriesRequest(
      RaftPeerId leaderId, long callId, int entriesCount, SpanContextProto spanContext) {
    final RaftRpcRequestProto.Builder rpc = RaftRpcRequestProto.newBuilder()
        .setRequestorId(leaderId.toByteString())
        .setCallId(callId);
    if (spanContext != null && !spanContext.getContextMap().isEmpty()) {
      rpc.setSpanContext(spanContext);
    }
    final AppendEntriesRequestProto.Builder b = AppendEntriesRequestProto.newBuilder()
        .setServerRequest(rpc.build())
        .setLeaderTerm(1L)
        .setLeaderCommit(0L);
    for (int i = 0; i < entriesCount; i++) {
      b.addEntries(LogEntryProto.newBuilder().setTerm(1L).setIndex(i + 1L).build());
    }
    return b.build();
  }

  private static List<SpanData> submitClientRequestAndCollectNewSpans(boolean enableTracing)
      throws Exception {
    final int before = openTelemetryExtension.getSpans().size();

    final RaftServerImpl server = newRaftServerImpl(enableTracing);
    try {
      final RaftClientRequest request = newRaftClientRequest(RaftClientRequest.writeRequestType());

      try {
        server.submitClientRequestAsync(request);
      } catch (ServerNotReadyException ignored) {
        // server is not running; only verifying span emission
      }
    } finally {
      server.close();
    }

    final List<SpanData> after = openTelemetryExtension.getSpans();
    return new ArrayList<>(after.subList(before, after.size()));
  }

  private static RaftServerImpl newRaftServerImpl(boolean enableTracing) throws Exception {
    final RaftGroup group = RaftGroup.emptyGroup();
    final StateMachine sm = new SimpleStateMachine4Testing();
    final RaftServerProxy proxy = mock(RaftServerProxy.class);
    when(proxy.getId()).thenReturn(RaftPeerId.valueOf("peer1"));
    final RaftProperties properties = new RaftProperties();
    TraceConfigKeys.setEnabled(properties, enableTracing);
    when(proxy.getProperties()).thenReturn(properties);
    when(proxy.getThreadGroup()).thenReturn(new ThreadGroup("test"));
    return new RaftServerImpl(group, sm, proxy, RaftStorage.StartupOption.FORMAT);
  }

  private static RaftClientRequest newRaftClientRequest(RaftClientRequest.Type type) {
    final Span clientSpan =
        openTelemetryExtension.getOpenTelemetry().getTracer("test")
            .spanBuilder("client-span")
            .setSpanKind(SpanKind.CLIENT)
            .startSpan();
    try {
      final Context clientContext = Context.current().with(clientSpan);
      return RaftClientRequest.newBuilder()
          .setClientId(ClientId.randomId())
          .setServerId(RaftPeerId.valueOf("s0"))
          .setGroupId(RaftGroupId.randomId())
          .setCallId(1L)
          .setType(type)
          .setSpanContext(TraceUtils.injectContextToProto(clientContext))
          .build();
    } finally {
      clientSpan.end();
    }
  }

  private long randomCallId() {
    return (long) (Math.random() * 100);
  }

}

