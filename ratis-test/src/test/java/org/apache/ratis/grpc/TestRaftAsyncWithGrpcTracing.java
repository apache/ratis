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

import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension;
import io.opentelemetry.sdk.trace.data.SpanData;
import org.apache.ratis.RaftAsyncTests;
import org.apache.ratis.trace.TraceConfigKeys;
import org.apache.ratis.trace.TraceUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;

@Timeout(100)
public class TestRaftAsyncWithGrpcTracing extends RaftAsyncTests<MiniRaftClusterWithGrpc>
    implements MiniRaftClusterWithGrpc.FactoryGet {
  {
    getProperties().setBoolean(TraceConfigKeys.ENABLED_KEY, true);
    TraceUtils.setTracingEnabled(true);
  }

  @RegisterExtension
  private static final OpenTelemetryExtension openTelemetryExtension =
      OpenTelemetryExtension.create();

  /**
   * Verifies traceAsyncRpcSend creates CLIENT spans when tracing is enabled.
   * testBasicAppendEntriesAsync uses client.async().send() which goes through AsyncImpl.send().
   */
  @Test
  public void testBasicAppendEntriesAsync() throws Exception {
    super.testBasicAppendEntriesAsync();
    List<SpanData> spans = openTelemetryExtension.getSpans();
    assertTrue(
        spans.stream().anyMatch(s -> s.getKind() == SpanKind.CLIENT),
        "Expected at least one span with SpanKind.CLIENT (from traceAsyncRpcSend)"
    );
    assertTrue(
        spans.stream().anyMatch(s -> s.getKind() == SpanKind.SERVER),
        "Expected at least one span with SpanKind.SERVER"
    );
  }

  @Test
  public void testWithLoadAsync() throws Exception {
    super.testWithLoadAsync();
    List<SpanData> spans = openTelemetryExtension.getSpans();
    assertTrue(
        spans.stream().anyMatch(s -> s.getKind() == SpanKind.CLIENT),
        "Expected at least one span with SpanKind.CLIENT (from traceAsyncRpcSend)"
    );
    assertTrue(
        spans.stream().anyMatch(s -> s.getKind() == SpanKind.SERVER),
        "Expected at least one span with SpanKind.SERVER"
    );
  }
}