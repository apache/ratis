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

import java.util.function.Consumer;
import org.apache.ratis.protocol.exceptions.ServerNotReadyException;
import org.apache.ratis.protocol.exceptions.TimeoutIOException;
import org.apache.ratis.security.TlsConf.TrustManagerConf;
import org.apache.ratis.security.TlsConf.CertificatesConf;
import org.apache.ratis.security.TlsConf.PrivateKeyConf;
import org.apache.ratis.security.TlsConf.KeyManagerConf;
import org.apache.ratis.thirdparty.io.grpc.ManagedChannel;
import org.apache.ratis.thirdparty.io.grpc.Metadata;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.thirdparty.io.grpc.StatusRuntimeException;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.SslContextBuilder;
import org.apache.ratis.util.IOUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.LogUtils;
import org.apache.ratis.util.ReflectionUtils;
import org.apache.ratis.util.function.CheckedSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManager;
import javax.net.ssl.TrustManager;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

public interface GrpcUtil {
  Logger LOG = LoggerFactory.getLogger(GrpcUtil.class);

  Metadata.Key<String> EXCEPTION_TYPE_KEY =
      Metadata.Key.of("exception-type", Metadata.ASCII_STRING_MARSHALLER);
  Metadata.Key<byte[]> EXCEPTION_OBJECT_KEY =
      Metadata.Key.of("exception-object-bin", Metadata.BINARY_BYTE_MARSHALLER);
  Metadata.Key<String> CALL_ID =
      Metadata.Key.of("call-id", Metadata.ASCII_STRING_MARSHALLER);
  Metadata.Key<String> HEARTBEAT =
      Metadata.Key.of("heartbeat", Metadata.ASCII_STRING_MARSHALLER);

  static StatusRuntimeException wrapException(Throwable t) {
    return wrapException(t, -1);
  }

  static StatusRuntimeException wrapException(Throwable t, long callId) {
    t = JavaUtils.unwrapCompletionException(t);
    Metadata trailers = new StatusRuntimeExceptionMetadataBuilder(t)
        .addCallId(callId)
        .build();
    return wrapException(t, trailers);
  }

  static StatusRuntimeException wrapException(Throwable t, long callId, boolean isHeartbeat) {
    t = JavaUtils.unwrapCompletionException(t);
    Metadata trailers = new StatusRuntimeExceptionMetadataBuilder(t)
        .addCallId(callId)
        .addIsHeartbeat(isHeartbeat)
        .build();
    return wrapException(t, trailers);
  }

  static StatusRuntimeException wrapException(Throwable t, Metadata trailers) {
    return new StatusRuntimeException(
        Status.INTERNAL.withCause(t).withDescription(t.getMessage()), trailers);
  }

  static Throwable unwrapThrowable(Throwable t) {
    if (t instanceof StatusRuntimeException) {
      final Throwable unwrapped = tryUnwrapThrowable((StatusRuntimeException)t);
      if (unwrapped != null) {
        return unwrapped;
      }
    }
    return t;
  }

  static IOException unwrapException(StatusRuntimeException se) {
    final Throwable t = tryUnwrapThrowable(se);
    return t instanceof IOException? (IOException) t: new IOException(t != null? t: se);
  }

  static Throwable tryUnwrapThrowable(StatusRuntimeException se) {
    final Status status = se.getStatus();
    if (status != null && status.getCode() == Status.Code.DEADLINE_EXCEEDED) {
      return new TimeoutIOException(status.getDescription(), se);
    }

    final Metadata trailers = se.getTrailers();
    if (trailers == null) {
      return null;
    }

    final byte[] bytes = trailers.get(EXCEPTION_OBJECT_KEY);
    if (bytes != null) {
      try {
        return IOUtils.bytes2Object(bytes, Throwable.class);
      } catch (Exception e) {
        se.addSuppressed(e);
      }
    }

    if (status != null) {
      final String className = trailers.get(EXCEPTION_TYPE_KEY);
      if (className != null) {
        try {
          final Class<? extends Throwable> clazz = Class.forName(className).asSubclass(Throwable.class);
          final Throwable unwrapped = ReflectionUtils.instantiateException(clazz, status.getDescription());
          return unwrapped.initCause(se);
        } catch (Throwable e) {
          se.addSuppressed(e);
          return new IOException(se);
        }
      }
    }
    return null;
  }

  static long getCallId(Throwable t) {
    if (t instanceof StatusRuntimeException) {
      final Metadata trailers = ((StatusRuntimeException)t).getTrailers();
      String callId = trailers.get(CALL_ID);
      return callId != null ? Integer.parseInt(callId) : -1;
    }
    return -1;
  }

  static boolean isHeartbeat(Throwable t) {
    if (t instanceof StatusRuntimeException) {
      final Metadata trailers = ((StatusRuntimeException)t).getTrailers();
      String isHeartbeat = trailers != null ? trailers.get(HEARTBEAT) : null;
      return isHeartbeat != null && Boolean.valueOf(isHeartbeat);
    }
    return false;
  }

  static IOException unwrapIOException(Throwable t) {
    final IOException e;
    if (t instanceof StatusRuntimeException) {
      e = GrpcUtil.unwrapException((StatusRuntimeException) t);
    } else {
      e = IOUtils.asIOException(t);
    }
    return e;
  }

  static <REPLY, REPLY_PROTO> void asyncCall(
      StreamObserver<REPLY_PROTO> responseObserver,
      CheckedSupplier<CompletableFuture<REPLY>, IOException> supplier,
      Function<REPLY, REPLY_PROTO> toProto) {
    asyncCall(responseObserver, supplier, toProto, throwable -> {});
  }

  static <REPLY, REPLY_PROTO> void asyncCall(
          StreamObserver<REPLY_PROTO> responseObserver,
          CheckedSupplier<CompletableFuture<REPLY>, IOException> supplier,
          Function<REPLY, REPLY_PROTO> toProto,
          Consumer<Throwable> warning) {
    try {
      supplier.get().whenComplete((reply, exception) -> {
        if (exception != null) {
          warning.accept(exception);
          responseObserver.onError(GrpcUtil.wrapException(exception));
        } else {
          responseObserver.onNext(toProto.apply(reply));
          responseObserver.onCompleted();
        }
      });
    } catch (Exception e) {
      warning.accept(e);
      responseObserver.onError(GrpcUtil.wrapException(e));
    }
  }

  static void warn(Logger log, Supplier<String> message, Throwable t) {
    LogUtils.warn(log, message, unwrapThrowable(t), StatusRuntimeException.class, ServerNotReadyException.class);
  }

  class StatusRuntimeExceptionMetadataBuilder {
    private Metadata trailers = new Metadata();

    StatusRuntimeExceptionMetadataBuilder(Throwable t) {
      trailers.put(EXCEPTION_TYPE_KEY, t.getClass().getCanonicalName());
      trailers.put(EXCEPTION_OBJECT_KEY, IOUtils.object2Bytes(t));
    }

    StatusRuntimeExceptionMetadataBuilder addCallId(long callId) {
      if (callId > 0) {
        trailers.put(CALL_ID, String.valueOf(callId));
      }
      return this;
    }

    StatusRuntimeExceptionMetadataBuilder addIsHeartbeat(boolean isHeartbeat) {
      trailers.put(HEARTBEAT, String.valueOf(isHeartbeat));
      return this;
    }

    Metadata build() {
      return trailers;
    }
  }

  /**
   * Tries to gracefully shut down the managed channel. Falls back to forceful shutdown if
   * graceful shutdown times out.
   */
  static void shutdownManagedChannel(ManagedChannel managedChannel) {
    // Close the gRPC managed-channel if not shut down already.
    if (!managedChannel.isShutdown()) {
      try {
        managedChannel.shutdown();
        if (!managedChannel.awaitTermination(3, TimeUnit.SECONDS)) {
          LOG.warn("Timed out gracefully shutting down connection: {}. ", managedChannel);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        LOG.error("Unexpected exception while waiting for channel termination", e);
      }
    }

    // Forceful shut down if still not terminated.
    if (!managedChannel.isTerminated()) {
      try {
        managedChannel.shutdownNow();
        if (!managedChannel.awaitTermination(2, TimeUnit.SECONDS)) {
          LOG.warn("Timed out forcefully shutting down connection: {}. ", managedChannel);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        LOG.error("Unexpected exception while waiting for channel termination", e);
      }
    }
  }

  static SslContextBuilder initSslContextBuilderForServer(KeyManagerConf keyManagerConfig) {
    final KeyManager keyManager = keyManagerConfig.getKeyManager();
    if (keyManager != null) {
      return SslContextBuilder.forServer(keyManager);
    }
    final PrivateKeyConf privateKey = keyManagerConfig.getPrivateKey();
    final CertificatesConf certificates = keyManagerConfig.getKeyCertificates();

    if (keyManagerConfig.isFileBased()) {
      return SslContextBuilder.forServer(certificates.getFile(), privateKey.getFile());
    } else {
      return SslContextBuilder.forServer(privateKey.get(), certificates.get());
    }
  }

  static void setTrustManager(SslContextBuilder b, TrustManagerConf trustManagerConfig) {
    if (trustManagerConfig == null) {
      return;
    }
    final TrustManager trustManager = trustManagerConfig.getTrustManager();
    if (trustManager != null) {
      b.trustManager(trustManager);
      return;
    }
    final CertificatesConf certificates = trustManagerConfig.getTrustCertificates();
    if (certificates.isFileBased()) {
      b.trustManager(certificates.getFile());
    } else {
      b.trustManager(certificates.get());
    }
  }

  static void setKeyManager(SslContextBuilder b, KeyManagerConf keyManagerConfig) {
    if (keyManagerConfig == null) {
      return;
    }
    final KeyManager keyManager = keyManagerConfig.getKeyManager();
    if (keyManager != null) {
      b.keyManager(keyManager);
      return;
    }
    final PrivateKeyConf privateKey = keyManagerConfig.getPrivateKey();
    final CertificatesConf certificates = keyManagerConfig.getKeyCertificates();

    if (keyManagerConfig.isFileBased()) {
      b.keyManager(certificates.getFile(), privateKey.getFile());
    } else {
      b.keyManager(privateKey.get(), certificates.get());
    }
  }
}
