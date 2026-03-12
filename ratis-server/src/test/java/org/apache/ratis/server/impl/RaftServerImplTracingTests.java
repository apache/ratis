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
import org.apache.ratis.trace.TraceConfigKeys;
import org.apache.ratis.trace.TraceUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
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
    final List<SpanData> spans = submitClientRequestAndCollectNewSpans(true);
    assertEquals(2, spans.size());
    assertTrue(
        spans.stream().anyMatch(s -> s.getKind() == SpanKind.CLIENT && s.getName().equals("client-span")),
        "Expected at least one span with SpanKind.CLIENT"
    );
    assertTrue(
        spans.stream().anyMatch(s -> s.getKind() == SpanKind.SERVER
            && s.getName().equals("raft.server.submitClientRequestAsync")),
        "Expected at least one span with SpanKind.SERVER"
    );

  }

  @Test
  public void testSubmitClientRequestAsyncTracingDisabled() throws Exception {
    final List<SpanData> spans = submitClientRequestAndCollectNewSpans(false);
    // Even when server-side tracing is disabled, we still emit the client span used to
    // generate the propagated context.
    assertEquals(1, spans.size());
    assertTrue(
        spans.stream().noneMatch(s -> s.getKind() == SpanKind.SERVER
            && s.getName().equals("raft.server.submitClientRequestAsync")),
        "Expected no SERVER span when tracing is disabled"
    );
    assertTrue(
        spans.stream().anyMatch(s -> s.getKind() == SpanKind.CLIENT && s.getName().equals("client-span")),
        "Expected at least one span with SpanKind.CLIENT"
    );
  }

  private static List<SpanData> submitClientRequestAndCollectNewSpans(boolean enableTracing)
      throws Exception {
    final int before = openTelemetryExtension.getSpans().size();

    final RaftServerImpl server = newRaftServerImpl(enableTracing);
    try {
      final RaftClientRequest request = newRaftClientRequest(RaftClientRequest.writeRequestType());

      try {
        server.submitClientRequestAsync(request);
      } catch (ServerNotReadyException ignored) {
        // server is not running; only verifying span emission
      }
    } finally {
      server.close();
    }

    final List<SpanData> after = openTelemetryExtension.getSpans();
    return new ArrayList<>(after.subList(before, after.size()));
  }

  private static RaftServerImpl newRaftServerImpl(boolean enableTracing) throws Exception {
    final RaftGroup group = RaftGroup.emptyGroup();
    final StateMachine sm = new SimpleStateMachine4Testing();
    final RaftServerProxy proxy = mock(RaftServerProxy.class);
    when(proxy.getId()).thenReturn(RaftPeerId.valueOf("peer1"));
    final RaftProperties properties = new RaftProperties();
    TraceConfigKeys.setEnabled(properties, enableTracing);
    when(proxy.getProperties()).thenReturn(properties);
    when(proxy.getThreadGroup()).thenReturn(new ThreadGroup("test"));
    return new RaftServerImpl(group, sm, proxy, RaftStorage.StartupOption.FORMAT);
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

