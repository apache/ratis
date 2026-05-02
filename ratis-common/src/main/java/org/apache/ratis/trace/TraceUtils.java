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
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.SpanContextProto;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.function.CheckedSupplier;
import org.apache.ratis.util.VersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/** Common OpenTelemetry utilities shared by {@link TraceClient} and {@link TraceServer}. */
public final class TraceUtils {

  private static final AtomicReference<Tracer> TRACER = new AtomicReference<>();
  private static final Logger LOG = LoggerFactory.getLogger(TraceUtils.class);

  private TraceUtils() {
  }

  public static Tracer getGlobalTracer() {
    return TRACER.get();
  }

  /**
   * Initializes the global tracer from configuration when tracing is enabled, or clears it when
   * disabled. Call from {@link org.apache.ratis.server.RaftServer} and
   * {@link org.apache.ratis.client.RaftClient} construction so tracing follows
   * {@link TraceConfigKeys}.
   *
   * @param properties raft configuration; tracing is on when {@link TraceConfigKeys#enabled} is true
   */
  public static void setTracerWhenEnabled(RaftProperties properties) {
    setTracerWhenEnabled(TraceConfigKeys.enabled(properties));
  }

  /**
   * Enables or disables the tracer without reading {@link RaftProperties}. Intended for tests and
   * simple toggles; production code should prefer {@link #setTracerWhenEnabled(RaftProperties)}.
   *
   * @param enabled when true, lazily obtains the OpenTelemetry tracer; when false, clears it
   */
  public static void setTracerWhenEnabled(boolean enabled) {
    if (enabled) {
      TRACER.updateAndGet(previous -> previous != null ? previous
          : GlobalOpenTelemetry.getTracer("org.apache.ratis", VersionInfo.getSoftwareInfoVersion()));
    } else {
      TRACER.set(null);
    }
  }

  public static boolean isEnabled() {
    return TRACER.get() != null;
  }

  /**
   * Traces an asynchronous operation represented by a {@link CompletableFuture}. The returned future
   * completes with the same outcome as the supplied future; the span is ended when that future
   * completes.
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
        // https://s.apache.org/completionexception — unwrap CompletionException for callers
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
