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
package org.apache.ratis.trace;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Context;
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.proto.RaftProtos.RaftRpcRequestProto;
import org.apache.ratis.proto.RaftProtos.SpanContextProto;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.util.function.CheckedSupplier;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/** Server-side OpenTelemetry helpers. */
public final class TraceServer {
  private TraceServer() {
  }

  /**
   * Traces {@code submitClientRequestAsync} when tracing is enabled.
   */
  public static <T, THROWABLE extends Throwable> CompletableFuture<T> traceAsyncMethod(
      CheckedSupplier<CompletableFuture<T>, THROWABLE> action,
      RaftClientRequest request, String memberId, String spanName) throws THROWABLE {
    if (!TraceUtils.isEnabled()) {
      return action.get();
    }
    return TraceUtils.traceAsyncMethod(action,
        () -> createServerSpanFromClientRequest(request, memberId, spanName));
  }

  private static Span createServerSpanFromClientRequest(RaftClientRequest request, String memberId,
      String spanName) {
    final Context remoteContext = TraceUtils.extractContextFromProto(request.getSpanContext());
    final Span span = TraceUtils.getGlobalTracer()
        .spanBuilder(spanName)
        .setParent(remoteContext)
        .setSpanKind(SpanKind.SERVER)
        .startSpan();
    span.setAttribute(RatisAttributes.CLIENT_ID, String.valueOf(request.getClientId()));
    span.setAttribute(RatisAttributes.CALL_ID, String.valueOf(request.getCallId()));
    span.setAttribute(RatisAttributes.MEMBER_ID, memberId);
    return span;
  }

  /**
   * Traces follower handling of {@link AppendEntriesRequestProto} when the leader attached trace
   * context (client-originated) for replication.
   */
  public static <T> CompletableFuture<T> traceAppendEntriesAsync(
      CheckedSupplier<CompletableFuture<T>, IOException> action,
      AppendEntriesRequestProto request, String memberId) throws IOException {
    if (!TraceUtils.isEnabled()) {
      return action.get();
    }
    final RaftRpcRequestProto rpc = request.getServerRequest();
    final SpanContextProto spanContext = rpc.getSpanContext();
    // If the leader sent no parent span context, still trace as a root span
    // rather than skipping tracing entirely.
    final Context remoteContext = (spanContext == null || spanContext.getContextMap().isEmpty())
        ? Context.root()
        : TraceUtils.extractContextFromProto(spanContext);
    return TraceUtils.traceAsyncMethod(action, () -> {
      final Span span = TraceUtils.getGlobalTracer()
          .spanBuilder("raft.server.appendEntriesAsync")
          .setParent(remoteContext)
          .setSpanKind(SpanKind.INTERNAL)
          .startSpan();
      span.setAttribute(RatisAttributes.MEMBER_ID, memberId);
      span.setAttribute(RatisAttributes.PEER_ID, String.valueOf(RaftPeerId.valueOf(rpc.getRequestorId())));
      span.setAttribute(RatisAttributes.APPEND_ENTRIES_COUNT, (long) request.getEntriesCount());
      return span;
    });
  }
}
