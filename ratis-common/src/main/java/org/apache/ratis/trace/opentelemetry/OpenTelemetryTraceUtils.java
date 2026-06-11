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
package org.apache.ratis.trace.opentelemetry;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;
import org.apache.ratis.proto.RaftProtos.SpanContextProto;

import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/** OpenTelemetry-specific helpers. Callers using this class must provide OpenTelemetry jars. */
public final class OpenTelemetryTraceUtils {
  private OpenTelemetryTraceUtils() {
  }

  public static SpanContextProto injectContextToProto(Context context) {
    final Map<String, String> carrier = new TreeMap<>();
    getTextMapPropagator().inject(context, carrier, (map, key, value) -> map.put(key, value));
    return SpanContextProto.newBuilder().putAllContext(carrier).build();
  }

  static Context extractContextFromProto(SpanContextProto proto) {
    if (proto == null || proto.getContextMap().isEmpty()) {
      return Context.current();
    }
    return getTextMapPropagator().extract(Context.current(), proto, SpanContextGetter.INSTANCE);
  }

  private static TextMapPropagator getTextMapPropagator() {
    return GlobalOpenTelemetry.getPropagators().getTextMapPropagator();
  }

  private static final class SpanContextGetter implements TextMapGetter<SpanContextProto> {
    private static final SpanContextGetter INSTANCE = new SpanContextGetter();

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
}
