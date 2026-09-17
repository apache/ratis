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
package org.apache.ratis.trace.otel;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.proto.RaftProtos.RaftRpcRequestProto;
import org.apache.ratis.proto.RaftProtos.SpanContextProto;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.trace.SpanNames;
import org.apache.ratis.trace.TraceProvider;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.VersionInfo;
import org.apache.ratis.util.function.CheckedSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static org.apache.ratis.trace.otel.OTelRatisAttributes.APPEND_ENTRIES_COUNT;
import static org.apache.ratis.trace.otel.OTelRatisAttributes.CALL_ID;
import static org.apache.ratis.trace.otel.OTelRatisAttributes.CLIENT_ID;
import static org.apache.ratis.trace.otel.OTelRatisAttributes.MEMBER_ID;
import static org.apache.ratis.trace.otel.OTelRatisAttributes.OPERATION_NAME;
import static org.apache.ratis.trace.otel.OTelRatisAttributes.OPERATION_TYPE;
import static org.apache.ratis.trace.otel.OTelRatisAttributes.PEER_ID;

public final class OTelTraceProvider implements TraceProvider {
  private static final Logger LOG = LoggerFactory.getLogger(OTelTraceProvider.class);
  private static final String LEADER = "LEADER";

  private final Tracer tracer = Objects.requireNonNull(
      GlobalOpenTelemetry.getTracer("org.apache.ratis", VersionInfo.getSoftwareInfoVersion()),
      "tracer == null");

  @Override
  public SpanContextProto injectContextToProto() {
    return OTelTraceUtils.injectContextToProto();
  }

  @Override
  public <T, THROWABLE extends Throwable> CompletableFuture<T> traceClientSend(
      CheckedSupplier<CompletableFuture<T>, THROWABLE> action,
      RaftClientRequest.Type type, RaftPeerId server) throws THROWABLE {
    return traceAsyncMethod(action, () -> createClientOperationSpan(type, server, SpanNames.ASYNC_SEND));
  }

  @Override
  public <T, THROWABLE extends Throwable> CompletableFuture<T> traceServerRequest(
      CheckedSupplier<CompletableFuture<T>, THROWABLE> action,
      RaftClientRequest request, String memberId, String spanName) throws THROWABLE {
    return traceAsyncMethod(action, () -> createServerSpanFromClientRequest(request, memberId, spanName));
  }

  @Override
  public <T> CompletableFuture<T> traceAppendEntries(
      CheckedSupplier<CompletableFuture<T>, IOException> action,
      AppendEntriesRequestProto request, String memberId) throws IOException {
    final RaftRpcRequestProto rpc = request.getServerRequest();
    final SpanContextProto spanContext = rpc.getSpanContext();
    final Context remoteContext = (spanContext == null || spanContext.getContextMap().isEmpty())
        ? Context.root()
        : OTelTraceUtils.extractContextFromProto(spanContext);
    return traceAsyncMethod(action, () -> {
      final Span span = tracer.spanBuilder(SpanNames.APPEND_ENTRIES_ASYNC)
          .setParent(remoteContext)
          .setSpanKind(SpanKind.INTERNAL)
          .startSpan();
      span.setAttribute(MEMBER_ID, memberId);
      span.setAttribute(PEER_ID, String.valueOf(RaftPeerId.valueOf(rpc.getRequestorId())));
      span.setAttribute(APPEND_ENTRIES_COUNT, request.getEntriesCount());
      return span;
    });
  }

  private Span createClientOperationSpan(RaftClientRequest.Type type, RaftPeerId server, String spanName) {
    Preconditions.assertNotNull(spanName, () -> "Span name cannot be null");
    Preconditions.assertTrue(!spanName.isEmpty(), "Span name should not be empty");
    final String peerId = server == null ? LEADER : String.valueOf(server);
    final Span span = tracer.spanBuilder(spanName)
        .setSpanKind(SpanKind.CLIENT)
        .startSpan();
    span.setAttribute(PEER_ID, peerId);
    span.setAttribute(OPERATION_NAME, spanName);
    span.setAttribute(OPERATION_TYPE, String.valueOf(type));
    return span;
  }

  private Span createServerSpanFromClientRequest(RaftClientRequest request, String memberId, String spanName) {
    final Context remoteContext = OTelTraceUtils.extractContextFromProto(request.getSpanContext());
    final Span span = tracer.spanBuilder(spanName)
        .setParent(remoteContext)
        .setSpanKind(SpanKind.SERVER)
        .startSpan();
    span.setAttribute(CLIENT_ID, String.valueOf(request.getClientId()));
    span.setAttribute(CALL_ID, String.valueOf(request.getCallId()));
    span.setAttribute(MEMBER_ID, memberId);
    return span;
  }

  @SuppressWarnings("try")
  private static <T, THROWABLE extends Throwable> CompletableFuture<T> traceAsyncMethod(
      CheckedSupplier<CompletableFuture<T>, THROWABLE> action, Supplier<Span> spanSupplier) throws THROWABLE {
    final Span span = spanSupplier.get();
    try (Scope ignored = span.makeCurrent()) {
      final CompletableFuture<T> future;
      try {
        future = action.get();
      } catch (RuntimeException | Error e) {
        setError(span, e);
        span.end();
        throw e;
      } catch (Throwable t) {
        setError(span, t);
        span.end();
        throw JavaUtils.<THROWABLE>cast(t);
      }
      endSpan(future, span);
      return future;
    }
  }

  private static void endSpan(CompletableFuture<?> future, Span span) {
    addListener(future, (resp, error) -> {
      try {
        if (error != null) {
          setError(span, error);
        } else {
          span.setStatus(StatusCode.OK);
        }
      } catch (Throwable t) {
        LOG.error("Error setting span status, ending span anyway", t);
      } finally {
        span.end();
      }
    });
  }

  private static void setError(Span span, Throwable error) {
    span.recordException(error);
    span.setStatus(StatusCode.ERROR);
  }

  /**
   * This is method is used when you just want to add a listener to the given future. We will call
   * {@link CompletableFuture#whenComplete(BiConsumer)} to register the {@code action} to the
   * {@code future}. Ignoring the return value of a Future is considered as a bad practice as it may
   * suppress exceptions thrown from the code that completes the future, and this method will catch
   * all the exception thrown from the {@code action} to catch possible code bugs.
   * <p/>
   * And the error phone check will always report FutureReturnValueIgnored because every method in
   * the {@link CompletableFuture} class will return a new {@link CompletableFuture}, so you always
   * have one future that has not been checked. So we introduce this method and add a suppression
   * warnings annotation here.
   */
  @SuppressWarnings("FutureReturnValueIgnored")
  private static <T> void addListener(CompletableFuture<T> future,
      BiConsumer<? super T, ? super Throwable> action) {
    future.whenComplete((resp, error) -> {
      try {
        action.accept(resp, error == null ? null : JavaUtils.unwrapCompletionException(error));
      } catch (Throwable t) {
        LOG.error("Unexpected error caught when processing CompletableFuture", t);
      }
    });
  }

}
