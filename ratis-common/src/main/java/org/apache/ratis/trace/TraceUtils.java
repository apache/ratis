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
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.context.propagation.TextMapGetter;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.util.VersionInfo;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public final class TraceUtils {

  private TraceUtils() {
  }

  public static Tracer getGlobalTracer() {
    return GlobalOpenTelemetry.getTracer("org.apache.ratis", VersionInfo.getSoftwareInfoVersion());
  }

  /**
   * Create a span which parent is from remote, i.e, passed through rpc.
   * </p>
   * We will set the kind of the returned span to {@link SpanKind#SERVER}, as this should be the top
   * most span at server side.
   */
  public static Span createRemoteSpan(String name, Context ctx) {
    return getGlobalTracer().spanBuilder(name).setParent(ctx).setSpanKind(SpanKind.SERVER)
        .startSpan();
  }

  private static final TextMapPropagator PROPAGATOR =
      GlobalOpenTelemetry.getPropagators().getTextMapPropagator();

  public static RaftProtos.SpanContextProto injectContextToProto(Context context) {
    Map<String, String> carrier = new HashMap<>();
    PROPAGATOR.inject(context, carrier, (map, key, value) -> map.put(key, value));
    return RaftProtos.SpanContextProto.newBuilder().putAllContext(carrier).build();
  }

  public static Context extractContextFromProto(RaftProtos.SpanContextProto proto) {
    if (proto == null || proto.getContextMap().isEmpty()) {
      return Context.current();
    }
    final TextMapGetter<RaftProtos.SpanContextProto> getter = SpanContextGetter.INSTANCE;
    return PROPAGATOR.extract(Context.current(), proto, getter);
  }
}

class SpanContextGetter implements TextMapGetter<RaftProtos.SpanContextProto> {
  static final SpanContextGetter INSTANCE = new SpanContextGetter();

  @Override
  public Iterable<String> keys(RaftProtos.SpanContextProto carrier) {
    return carrier.getContextMap().keySet();
  }

  @Override
  public String get(RaftProtos.SpanContextProto carrier, String key) {
    return Optional.ofNullable(carrier).map(RaftProtos.SpanContextProto::getContextMap)
        .map(map -> map.get(key)).orElse(null);
  }
}