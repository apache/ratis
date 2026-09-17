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

import org.apache.ratis.thirdparty.io.grpc.Attributes;
import org.apache.ratis.thirdparty.io.grpc.Grpc;
import org.apache.ratis.thirdparty.io.grpc.InternalChannelz;
import org.apache.ratis.thirdparty.io.grpc.SecurityLevel;
import org.apache.ratis.thirdparty.io.grpc.ServerCredentials;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.thirdparty.io.grpc.internal.GrpcAttributes;
import org.apache.ratis.thirdparty.io.grpc.internal.ObjectPool;
import org.apache.ratis.thirdparty.io.grpc.netty.GrpcHttp2ConnectionHandler;
import org.apache.ratis.thirdparty.io.grpc.netty.InternalNettyServerCredentials;
import org.apache.ratis.thirdparty.io.grpc.netty.InternalProtocolNegotiationEvent;
import org.apache.ratis.thirdparty.io.grpc.netty.InternalProtocolNegotiator;
import org.apache.ratis.thirdparty.io.grpc.netty.InternalProtocolNegotiators;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelHandler;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.SslContext;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.SslHandler;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.SslHandshakeCompletionEvent;
import org.apache.ratis.thirdparty.io.netty.util.AsciiString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;

import java.net.SocketAddress;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/** Builds gRPC server credentials which report initial TLS handshake failures. */
public final class TlsHandshakeFailureServerCredentials {
  private static final Logger LOG =
      LoggerFactory.getLogger(TlsHandshakeFailureServerCredentials.class);
  private static final AsciiString HTTPS = AsciiString.of("https");

  private TlsHandshakeFailureServerCredentials() {}

  /**
   * Creates server credentials from the given TLS configuration and failure listener.
   *
   * @param tlsConfig the server TLS configuration
   * @param listener the listener for TLS handshake failures; it is invoked on a transport event-loop
   *                 thread and must not block
   * @return server credentials reporting TLS handshake failures
   */
  public static ServerCredentials create(
      GrpcTlsConfig tlsConfig, Consumer<Event> listener) {
    Objects.requireNonNull(tlsConfig, "tlsConfig");
    Objects.requireNonNull(listener, "listener");
    final SslContext sslContext = GrpcUtil.buildSslContextForServer(tlsConfig);
    return InternalNettyServerCredentials.create(new Factory(sslContext, listener));
  }

  private static boolean containsSslException(Throwable throwable) {
    for (Throwable cause = throwable; cause != null; cause = cause.getCause()) {
      if (cause instanceof SSLException) {
        return true;
      }
    }
    return false;
  }

  private static final class Factory implements InternalProtocolNegotiator.ServerFactory {
    private final SslContext sslContext;
    private final Consumer<Event> listener;

    private Factory(SslContext sslContext, Consumer<Event> listener) {
      this.sslContext = sslContext;
      this.listener = listener;
    }

    @Override
    public InternalProtocolNegotiator.ProtocolNegotiator newNegotiator(
        ObjectPool<? extends Executor> offloadExecutorPool) {
      return new Negotiator(sslContext, listener, offloadExecutorPool);
    }
  }

  private static final class Negotiator implements InternalProtocolNegotiator.ProtocolNegotiator {
    private final SslContext sslContext;
    private final Consumer<Event> listener;
    private final ObjectPool<? extends Executor> offloadExecutorPool;
    private final Executor executor;

    private Negotiator(SslContext sslContext, Consumer<Event> listener,
        ObjectPool<? extends Executor> offloadExecutorPool) {
      this.sslContext = sslContext;
      this.listener = listener;
      this.offloadExecutorPool = offloadExecutorPool;
      this.executor = offloadExecutorPool != null ? offloadExecutorPool.getObject() : null;
    }

    @Override
    public AsciiString scheme() {
      return HTTPS;
    }

    @Override
    public ChannelHandler newHandler(GrpcHttp2ConnectionHandler grpcHandler) {
      final ChannelHandler grpcNegotiationHandler =
          InternalProtocolNegotiators.grpcNegotiationHandler(grpcHandler);
      final ChannelHandler tlsHandler = new ServerTlsHandler(grpcNegotiationHandler, grpcHandler,
          sslContext, listener, executor);
      return InternalProtocolNegotiators.waitUntilActiveHandler(
          tlsHandler, grpcHandler.getNegotiationLogger());
    }

    @Override
    public void close() {
      if (offloadExecutorPool != null && executor != null) {
        offloadExecutorPool.returnObject(executor);
      }
    }
  }

  private static final class ServerTlsHandler
      extends InternalProtocolNegotiators.ProtocolNegotiationHandler {
    private final SslContext sslContext;
    private final Consumer<Event> listener;
    private final Executor executor;
    private final AtomicBoolean failureReported = new AtomicBoolean();

    private ServerTlsHandler(ChannelHandler next, GrpcHttp2ConnectionHandler grpcHandler,
        SslContext sslContext, Consumer<Event> listener, Executor executor) {
      super(next, grpcHandler.getNegotiationLogger());
      this.sslContext = sslContext;
      this.listener = listener;
      this.executor = executor;
    }

    @Override
    protected void handlerAdded0(ChannelHandlerContext context) {
      final SSLEngine sslEngine = sslContext.newEngine(context.alloc());
      final SslHandler sslHandler = executor != null
          ? new SslHandler(sslEngine, false, executor)
          : new SslHandler(sslEngine, false);
      context.pipeline().addBefore(context.name(), null, sslHandler);
    }

    @Override
    protected void userEventTriggered0(ChannelHandlerContext context, Object event)
        throws Exception {
      if (!(event instanceof SslHandshakeCompletionEvent)) {
        super.userEventTriggered0(context, event);
        return;
      }

      final SslHandshakeCompletionEvent handshakeEvent = (SslHandshakeCompletionEvent) event;
      if (!handshakeEvent.isSuccess()) {
        final Throwable cause = handshakeEvent.cause();
        if (containsSslException(cause)) {
          notifyListener(context, cause);
        }
        context.fireExceptionCaught(cause);
        return;
      }

      final SslHandler sslHandler = context.pipeline().get(SslHandler.class);
      final String protocol = sslHandler.applicationProtocol();
      if (!sslContext.applicationProtocolNegotiator().protocols().contains(protocol)) {
        final Exception cause = Status.UNAVAILABLE
            .withDescription(
                "Failed protocol negotiation: Unable to find compatible protocol for " + protocol)
            .asException();
        notifyListener(context, cause);
        context.fireExceptionCaught(cause);
        return;
      }
      propagateTlsComplete(context, sslHandler.engine().getSession());
    }

    private void notifyListener(ChannelHandlerContext context, Throwable cause) {
      if (!failureReported.compareAndSet(false, true)) {
        return;
      }
      try {
        listener.accept(new Event(cause,
            context.channel().localAddress(), context.channel().remoteAddress(), true));
      } catch (Throwable listenerFailure) {
        LOG.warn("TLS handshake failure listener threw an exception", listenerFailure);
      }
    }

    private void propagateTlsComplete(ChannelHandlerContext context, SSLSession session) {
      final Attributes attributes =
          InternalProtocolNegotiationEvent.getAttributes(getProtocolNegotiationEvent()).toBuilder()
              .set(GrpcAttributes.ATTR_SECURITY_LEVEL, SecurityLevel.PRIVACY_AND_INTEGRITY)
              .set(Grpc.TRANSPORT_ATTR_SSL_SESSION, session)
              .build();
      replaceProtocolNegotiationEvent(InternalProtocolNegotiationEvent.withSecurity(
          InternalProtocolNegotiationEvent.withAttributes(
              getProtocolNegotiationEvent(), attributes),
          new InternalChannelz.Security(new InternalChannelz.Tls(session))));
      fireProtocolNegotiationEvent(context);
    }
  }

  /** Information about a failed TLS handshake. */
  public static final class Event {
    private final Throwable cause;
    private final SocketAddress localAddress;
    private final SocketAddress remoteAddress;
    private final boolean inbound;

    Event(Throwable cause, SocketAddress localAddress,
        SocketAddress remoteAddress, boolean inbound) {
      this.cause = Objects.requireNonNull(cause, "cause");
      this.localAddress = localAddress;
      this.remoteAddress = remoteAddress;
      this.inbound = inbound;
    }

    public Throwable getCause() {
      return cause;
    }

    public SocketAddress getLocalAddress() {
      return localAddress;
    }

    public SocketAddress getRemoteAddress() {
      return remoteAddress;
    }

    public boolean isInbound() {
      return inbound;
    }
  }
}
