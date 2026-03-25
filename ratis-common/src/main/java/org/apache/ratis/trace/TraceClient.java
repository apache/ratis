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
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.function.CheckedSupplier;

import java.util.concurrent.CompletableFuture;

/** Client-side OpenTelemetry helpers. */
public final class TraceClient {
  private static final String LEADER = "LEADER";

  private TraceClient() {
  }

  /**
   * Traces an asynchronous client send ({@code Async::send}) when tracing is enabled.
   */
  public static <T, THROWABLE extends Throwable> CompletableFuture<T> asyncSend(
      CheckedSupplier<CompletableFuture<T>, THROWABLE> action,
      RaftClientRequest.Type type, RaftPeerId server) throws THROWABLE {
    if (!TraceUtils.isEnabled()) {
      return action.get();
    }
    return TraceUtils.traceAsyncMethod(action,
        () -> createClientOperationSpan(type, server, "Async::send"));
  }

  private static Span createClientOperationSpan(RaftClientRequest.Type type, RaftPeerId server,
      String spanName) {
    Preconditions.assertNotNull(spanName, () -> "Span name cannot be null");
    Preconditions.assertTrue(!spanName.isEmpty(), "Span name should not be empty");
    String peerId = server == null ? LEADER : String.valueOf(server);
    final Span span = TraceUtils.getGlobalTracer()
        .spanBuilder(spanName)
        .setSpanKind(SpanKind.CLIENT)
        .startSpan();
    span.setAttribute(RatisAttributes.PEER_ID, peerId);
    span.setAttribute(RatisAttributes.OPERATION_NAME, spanName);
    span.setAttribute(RatisAttributes.OPERATION_TYPE, String.valueOf(type));
    return span;
  }
}
