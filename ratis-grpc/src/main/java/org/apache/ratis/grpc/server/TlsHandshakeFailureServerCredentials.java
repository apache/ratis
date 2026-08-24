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
package org.apache.ratis.grpc.server;

import org.apache.ratis.grpc.TlsHandshakeFailureEvent;
import org.apache.ratis.grpc.TlsHandshakeFailureListener;
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

import java.util.Objects;
import java.util.concurrent.Executor;

/** Builds gRPC server credentials which report initial TLS handshake failures. */
final class TlsHandshakeFailureServerCredentials {
  private static final Logger LOG =
      LoggerFactory.getLogger(TlsHandshakeFailureServerCredentials.class);
  private static final AsciiString HTTPS = AsciiString.of("https");

  private TlsHandshakeFailureServerCredentials() {}

  static ServerCredentials create(SslContext sslContext, TlsHandshakeFailureListener listener) {
    Objects.requireNonNull(sslContext, "sslContext");
    Objects.requireNonNull(listener, "listener");
    if (!sslContext.isServer()) {
      throw new IllegalArgumentException("Client SSL context cannot be used for a server");
    }
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
    private final TlsHandshakeFailureListener listener;

    private Factory(SslContext sslContext, TlsHandshakeFailureListener listener) {
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
    private final TlsHandshakeFailureListener listener;
    private final ObjectPool<? extends Executor> offloadExecutorPool;
    private final Executor executor;

    private Negotiator(SslContext sslContext, TlsHandshakeFailureListener listener,
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
          sslContext, listener, offloadExecutorPool);
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
    private final TlsHandshakeFailureListener listener;
    private final Executor executor;
    private boolean failureReported;

    private ServerTlsHandler(ChannelHandler next, GrpcHttp2ConnectionHandler grpcHandler,
        SslContext sslContext, TlsHandshakeFailureListener listener,
        ObjectPool<? extends Executor> offloadExecutorPool) {
      super(next, grpcHandler.getNegotiationLogger());
      this.sslContext = sslContext;
      this.listener = listener;
      this.executor = offloadExecutorPool != null ? offloadExecutorPool.getObject() : null;
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
      if (!sslContext.applicationProtocolNegotiator().protocols()
          .contains(sslHandler.applicationProtocol())) {
        final RuntimeException cause = Status.UNAVAILABLE
            .withDescription("Failed protocol negotiation: Unable to find compatible protocol")
            .asRuntimeException();
        notifyListener(context, cause);
        context.fireExceptionCaught(cause);
        return;
      }
      propagateTlsComplete(context, sslHandler.engine().getSession());
    }

    private void notifyListener(ChannelHandlerContext context, Throwable cause) {
      if (failureReported) {
        return;
      }
      failureReported = true;
      try {
        listener.onFailure(new TlsHandshakeFailureEvent(cause,
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
}
