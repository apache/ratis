/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension;
import io.opentelemetry.sdk.trace.data.SpanData;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class TestTraceUtils {

  @RegisterExtension
  private static final OpenTelemetryExtension openTelemetryExtension =
      OpenTelemetryExtension.create();

  private void runTraceAsyncAndAssertClientSpan(boolean tracingEnabled, boolean expectClientSpan)
      throws Exception {
    TraceConfigKeys.setEnabled(TraceUtils.getProperties(), tracingEnabled);
    TraceUtils.traceAsyncImplSend(
        () -> CompletableFuture.completedFuture("ok"),
        null,
        RaftPeerId.valueOf("s0")
    ).get();

    List<SpanData> spans = openTelemetryExtension.getSpans();
    boolean hasClientSpan = spans.stream().anyMatch(s -> s.getKind() == SpanKind.CLIENT);
    if (expectClientSpan) {
      assertTrue(hasClientSpan, "Expected CLIENT span from traceAsyncRpcSend, got: " + spans);
    } else {
      assertFalse(hasClientSpan, "Expected no CLIENT span when tracing disabled, got: " + spans);
    }
  }

  @Test
  public void testTraceAsyncRpcSendCreatesClientSpan() throws Exception {
    runTraceAsyncAndAssertClientSpan(true, true);
  }

  @Test
  public void testTraceAsyncRpcSendCreatesClientSpanDisabled() throws Exception {
    runTraceAsyncAndAssertClientSpan(false, false);
  }
}
