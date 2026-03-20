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
package org.apache.ratis.grpc;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension;
import io.opentelemetry.sdk.trace.data.SpanData;
import org.apache.ratis.trace.TraceConfigKeys;
import org.apache.ratis.trace.TraceUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;

public class TestRetryCacheWithGrpcTracing
    extends TestRetryCacheWithGrpc {
  @RegisterExtension
  private static final OpenTelemetryExtension openTelemetryExtension =
      OpenTelemetryExtension.create();

  {
    getProperties().setBoolean(TraceConfigKeys.ENABLED_KEY, true);
    TraceConfigKeys.setEnabled(TraceUtils.getProperties(), true);
  }

  private List<SpanData> spans;

  @BeforeEach
  void setUpOpenTelemetry() {
    GlobalOpenTelemetry.resetForTest();
    GlobalOpenTelemetry.set(openTelemetryExtension.getOpenTelemetry());
  }

  @AfterEach
  void tearDownOpenTelemetry() {
    GlobalOpenTelemetry.resetForTest();
  }

  /**
   * Verifies traceAsyncRpcSend creates CLIENT spans when tracing is enabled.
   * Uses testInvalidateRepliedCalls which exercises client.async().send() (traceAsyncRpcSend path).
   * testBasicRetry uses rpc.sendRequest() (blocking) which bypasses the async tracing path.
   */
  @Test
  public void testBasicRetry() throws Exception {
//    Span span = openTelemetryExtension.getOpenTelemetry().getTracer("test")
//        .spanBuilder("test-span").startSpan();

    runWithNewCluster(3, cluster -> new InvalidateRepliedCallsTest(cluster).run());
    // span.end();
    // Thread.sleep(1000);

    long deadline = System.currentTimeMillis() + 10000;
    do {
      spans = openTelemetryExtension.getSpans();
      if (!spans.isEmpty()) break;
      Thread.sleep(100);
    } while (System.currentTimeMillis() < deadline);

    LOG.info("Collected spans: {}", spans);

    assertTrue(
        spans.stream().anyMatch(s -> s.getKind() == SpanKind.CLIENT),
        "Expected at least one span with SpanKind.CLIENT (from traceAsyncRpcSend)"
    );

    assertTrue(
        spans.stream().anyMatch(s -> s.getKind() == SpanKind.SERVER),
        "Expected at least one span with SpanKind.SERVER"
    );

//    // this must fail as there are more than two spans created in the test
//    openTelemetryExtension.assertTraces().hasTracesSatisfyingExactly(
//        trace -> trace.hasSpansSatisfyingExactly(spanAssert -> spanAssert.hasName("raft.server.appendEntries")),
//        trace -> trace.hasSpansSatisfyingExactly(spanAssert -> spanAssert.hasName("test-appendEntries_emitsSpan")));
  }
}
