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

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.context.propagation.TextMapGetter;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.SpanContextProto;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.function.CheckedSupplier;
import org.apache.ratis.util.VersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public final class TraceUtils {

  private static final Tracer TRACER = GlobalOpenTelemetry.getTracer("org.apache.ratis",
      VersionInfo.getSoftwareInfoVersion());

  private static final Logger LOG = LoggerFactory.getLogger(TraceUtils.class);

  private static final RaftProperties PROPERTIES = new RaftProperties();
  private static final String DEFAULT_OPERATION = "DEFAULT_OPERATION";
  private static final String UNKNOWN_PEER = "UNKNOWN_PEER";

  private TraceUtils() {
  }

  public static Tracer getGlobalTracer() {
    return TRACER;
  }

  public static void setTracingEnabled(boolean enabled) {
    TraceConfigKeys.setEnabled(PROPERTIES, enabled);
  }

  /**
   * Trace an asynchronous operation represented by a {@link CompletableFuture}.
   * The returned future will complete with the same result or error as the original future,
   * but the provided {@code span} will be ended when the future completes.
   */
  static <T, THROWABLE extends Throwable> CompletableFuture<T> traceAsyncMethod(
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

  public static <T, THROWABLE extends Throwable> CompletableFuture<T> traceAsyncMethod(
      CheckedSupplier<CompletableFuture<T>, THROWABLE> action,
      RaftClientRequest request, String memberId, String spanName) throws THROWABLE {
    return traceAsyncMethod(action, () -> createServerSpanFromClientRequest(request, memberId, spanName));
  }

  public static <T, THROWABLE extends Throwable> CompletableFuture<T> traceAsyncMethodIfEnabled(
      boolean enabled,
      CheckedSupplier<CompletableFuture<T>, THROWABLE> action,
      RaftClientRequest request, String memberId, String spanName) throws THROWABLE {
    return enabled ? traceAsyncMethod(action, request, memberId, spanName) : action.get();
  }

  public static <T, THROWABLE extends Throwable> CompletableFuture<T> traceAsyncImplSend(
      CheckedSupplier<CompletableFuture<T>, THROWABLE> action,
      RaftClientRequest.Type type, RaftPeerId server) throws THROWABLE {
    return isTraceEnabled() ? traceAsyncClientSend(action, type, server, "AsyncImpl::send") : action.get();
  }

  private static boolean isTraceEnabled() {
    return TraceConfigKeys.enabled(PROPERTIES);
  }

  public static <T, THROWABLE extends Throwable> CompletableFuture<T> traceAsyncClientSend(
      CheckedSupplier<CompletableFuture<T>, THROWABLE> action,
      RaftClientRequest.Type type, RaftPeerId server, String spanName) throws THROWABLE {
    return traceAsyncMethod(action, () -> createClientOperationSpan(type, server, spanName));
  }

  private static Span createClientOperationSpan(RaftClientRequest.Type type, RaftPeerId server,
      String spanName) {
    String name = (spanName == null || spanName.isEmpty()) ? DEFAULT_OPERATION : spanName;
    String peerId = server == null ? UNKNOWN_PEER : String.valueOf(server);
    final Span span = getGlobalTracer()
        .spanBuilder(name)
        .setSpanKind(SpanKind.CLIENT)
        .startSpan();
    span.setAttribute(RatisAttributes.PEER_ID, peerId);
    span.setAttribute(RatisAttributes.OPERATION_NAME, name);
    span.setAttribute(RatisAttributes.OPERATION_TYPE, String.valueOf(type));
    return span;
  }

  private static Span createServerSpanFromClientRequest(RaftClientRequest request, String memberId, String spanName) {
    final Context remoteContext = extractContextFromProto(request.getSpanContext());
    final Span span = getGlobalTracer()
        .spanBuilder(spanName)
        .setParent(remoteContext)
        .setSpanKind(SpanKind.SERVER)
        .startSpan();
    span.setAttribute(RatisAttributes.CLIENT_ID, String.valueOf(request.getClientId()));
    span.setAttribute(RatisAttributes.CALL_ID, String.valueOf(request.getCallId()));
    span.setAttribute(RatisAttributes.MEMBER_ID, memberId);
    return span;
  }

  private static void endSpan(CompletableFuture<?> future, Span span) {
    if (span == null) {
      LOG.debug("Span is null, cannot trace the future {}", future);
      return;
    }
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

  public static void setError(Span span, Throwable error) {
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
        // See this post on stack overflow(shorten since the url is too long),
        // https://s.apache.org/completionexception
        // For a chain of CompletableFuture, only the first child CompletableFuture can get the
        // original exception, others will get a CompletionException, which wraps the original
        // exception. So here we unwrap it before passing it to the callback action.
        action.accept(resp, error == null ? null : JavaUtils.unwrapCompletionException(error));
      } catch (Throwable t) {
        LOG.error("Unexpected error caught when processing CompletableFuture", t);
      }
    });
  }

  private static final TextMapPropagator PROPAGATOR =
      GlobalOpenTelemetry.getPropagators().getTextMapPropagator();

  public static SpanContextProto injectContextToProto(Context context) {
    Map<String, String> carrier = new TreeMap<>();
    PROPAGATOR.inject(context, carrier, (map, key, value) -> map.put(key, value));
    return SpanContextProto.newBuilder().putAllContext(carrier).build();
  }

  public static Context extractContextFromProto(SpanContextProto proto) {
    if (proto == null || proto.getContextMap().isEmpty()) {
      return Context.current();
    }
    final TextMapGetter<SpanContextProto> getter = SpanContextGetter.INSTANCE;
    return PROPAGATOR.extract(Context.current(), proto, getter);
  }
}

class SpanContextGetter implements TextMapGetter<SpanContextProto> {
  static final SpanContextGetter INSTANCE = new SpanContextGetter();

  @Override
  public Iterable<String> keys(SpanContextProto carrier) {
    return carrier.getContextMap().keySet();
  }

  @Override
  public String get(SpanContextProto carrier, String key) {
    return Optional.ofNullable(carrier).map(SpanContextProto::getContextMap)
        .map(map -> map.get(key)).orElse(null);
  }

}