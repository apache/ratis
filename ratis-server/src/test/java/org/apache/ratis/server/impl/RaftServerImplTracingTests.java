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

  private List<SpanData> spans;

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
    RaftClientRequest r = newRaftClientRequest(RaftClientRequest.writeRequestType());

    // invoke submitClientRequestAsync
    Span span = openTelemetryExtension
        .getOpenTelemetry().getTracer("test").spanBuilder("test-appendEntries_emitsSpan")
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

    spans = openTelemetryExtension.getSpans();
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
            && s.getName().equals("test-appendEntries_emitsSpan")),
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
      return RaftClientRequest.newBuilder()
          .setClientId(ClientId.randomId())
          .setServerId(RaftPeerId.valueOf("s0"))
          .setGroupId(RaftGroupId.randomId())
          .setCallId(1L)
          .setType(type)
          .setSpanContext(TraceUtils.injectContextToProto(Context.current()))
          .build();
    } finally {
      clientSpan.end();
    }
  }
}

