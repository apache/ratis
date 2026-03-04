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
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.context.propagation.TextMapGetter;
import org.apache.ratis.proto.RaftProtos.SpanContextProto;
import org.apache.ratis.util.FutureUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.function.CheckedSupplier;
import org.apache.ratis.util.VersionInfo;

import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public final class TraceUtils {

  private static final Tracer TRACER = GlobalOpenTelemetry.getTracer("org.apache.ratis",
      VersionInfo.getSoftwareInfoVersion());

  private TraceUtils() {
  }

  public static Tracer getGlobalTracer() {
    return TRACER;
  }

  /**
   * Trace an asynchronous operation represented by a {@link CompletableFuture}.
   * The returned future will complete with the same result or error as the original future,
   * but the provided {@code span} will be ended when the future completes.
   */
  public static <T, THROWABLE extends Throwable> CompletableFuture<T>  traceAsyncMethod(
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
    FutureUtils.addListener(future, (resp, error) -> {
      if (error != null) {
        setError(span, error);
      } else {
        span.setStatus(StatusCode.OK);
      }
      span.end();
    });
  }

  public static void setError(Span span, Throwable error) {
    span.recordException(error);
    span.setStatus(StatusCode.ERROR);
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