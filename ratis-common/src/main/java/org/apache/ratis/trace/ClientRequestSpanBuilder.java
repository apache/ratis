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

import static org.apache.ratis.trace.RatisAttributes.ATTR_CALL_ID;
import static org.apache.ratis.trace.RatisAttributes.ATTR_CLIENT_INVOCATION_ID;
import static org.apache.ratis.trace.RatisAttributes.ATTR_MEMBER_ID;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Context;
import org.apache.ratis.proto.RaftProtos.SpanContextProto;
import org.apache.ratis.protocol.RaftClientRequest;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Construct {@link Span} instances originating from the client request.
 */
public class ClientRequestSpanBuilder implements Supplier<Span> {

  private String name;
  private Context remoteContext;
  private final Map<AttributeKey<?>, Object> attributes = new HashMap<>();

  @Override
  public Span get() {
    return build();
  }

  public ClientRequestSpanBuilder setAttributes(final RaftClientRequest request, final String memberId) {
    remoteContext = TraceUtils.extractContextFromProto(request.getSpanContext());
    setRequestAttributes(attributes, request, memberId);
    return this;
  }

  public ClientRequestSpanBuilder setSpanName(final String spanName) {
    this.name = spanName;
    return this;
  }

  public ClientRequestSpanBuilder setRemoteContext(final SpanContextProto spanContextProto) {
    remoteContext = TraceUtils.extractContextFromProto(spanContextProto);
    return this;
  }


  @SuppressWarnings("unchecked")
  public Span build() {
    final SpanBuilder builder = TraceUtils.getGlobalTracer().spanBuilder(name).setParent(remoteContext)
        .setSpanKind(SpanKind.SERVER);
    attributes.forEach((k, v) -> builder.setAttribute((AttributeKey<? super Object>) k, v));
    return builder.startSpan();
  }

  /**
   * Static utility method that performs the primary logic of this builder. It is visible to other
   * classes in this package so that other builders can use this functionality as a mix-in.
   * @param attributes the attributes map to be populated.
   */
  static void setRequestAttributes(final Map<AttributeKey<?>, Object> attributes,
      final RaftClientRequest request, final String memberId) {
    attributes.put(ATTR_CLIENT_INVOCATION_ID, String.valueOf(request.getClientId()));
    attributes.put(ATTR_CALL_ID, String.valueOf(request.getCallId()));
    attributes.put(ATTR_MEMBER_ID, memberId);
  }

}
