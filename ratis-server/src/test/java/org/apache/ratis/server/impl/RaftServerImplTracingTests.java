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
package org.apache.ratis.server.impl;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension;
import io.opentelemetry.sdk.trace.data.SpanData;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.ServerNotReadyException;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachine4Testing;
import org.apache.ratis.trace.TraceUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;

public class RaftServerImplTracingTests {

  @RegisterExtension
  private static final OpenTelemetryExtension openTelemetryExtension =
      OpenTelemetryExtension.create();

  @Test
  public void testSubmitClientRequestAsync() throws Exception {
    RaftGroup group = RaftGroup.emptyGroup();
    StateMachine sm = new SimpleStateMachine4Testing();
    RaftServerProxy proxy = mock(RaftServerProxy.class);
    when(proxy.getId()).thenReturn(RaftPeerId.valueOf("peer1"));
    when(proxy.getProperties()).thenReturn(new RaftProperties());
    when(proxy.getThreadGroup()).thenReturn(new ThreadGroup("test"));

    RaftServerImpl server = new RaftServerImpl(group, sm, proxy, RaftStorage.StartupOption.FORMAT);

    // build a minimal RaftClientRequest with a client span context;
    final RaftClientRequest r = newRaftClientRequest(RaftClientRequest.writeRequestType());
    final String testSpanName = "test-appendEntries_emitsSpan";

    // invoke submitClientRequestAsync
    Span span = openTelemetryExtension
        .getOpenTelemetry().getTracer("test").spanBuilder(testSpanName)
        .setSpanKind(SpanKind.INTERNAL)
        .startSpan();
          try {
            server.submitClientRequestAsync(r);
          } catch (ServerNotReadyException ignored) {
            // ignore the server is not running, because we're just trying to verify the span is emitted,
            // and the server not running is expected in this test
          } finally {
            span.end();
          }

    List<SpanData> spans = openTelemetryExtension.getSpans();
    assertEquals(3, spans.size());
    assertTrue(
        spans.stream().anyMatch(s -> s.getKind() == SpanKind.CLIENT && s.getName().equals("client-span")),
        "Expected at least one span with SpanKind.CLIENT"
    );
    assertTrue(
        spans.stream().anyMatch(s -> s.getKind() == SpanKind.SERVER
            && s.getName().equals("raft.server.submitClientRequestAsync")),
        "Expected at least one span with SpanKind.SERVER"
    );
    assertTrue(
        spans.stream().anyMatch(s -> s.getKind() == SpanKind.INTERNAL
            && s.getName().equals(testSpanName)),
        "Expected at least one span with SpanKind.INTERNAL"
    );

  }

  private static RaftClientRequest newRaftClientRequest(RaftClientRequest.Type type) {
    final Span clientSpan =
        openTelemetryExtension.getOpenTelemetry().getTracer("test")
            .spanBuilder("client-span")
            .setSpanKind(SpanKind.CLIENT)
            .startSpan();
    try {
      final Context clientContext = Context.current().with(clientSpan);
      return RaftClientRequest.newBuilder()
          .setClientId(ClientId.randomId())
          .setServerId(RaftPeerId.valueOf("s0"))
          .setGroupId(RaftGroupId.randomId())
          .setCallId(1L)
          .setType(type)
          .setSpanContext(TraceUtils.injectContextToProto(clientContext))
          .build();
    } finally {
      clientSpan.end();
    }
  }
}

