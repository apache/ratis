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

import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.ServerNotReadyException;
import org.apache.ratis.thirdparty.io.grpc.Metadata;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.thirdparty.io.grpc.StatusRuntimeException;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.apache.ratis.util.IOUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.LogUtils;
import org.apache.ratis.util.ReflectionUtils;
import org.apache.ratis.util.function.CheckedSupplier;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

public interface GrpcUtil {
  Metadata.Key<String> EXCEPTION_TYPE_KEY =
      Metadata.Key.of("exception-type", Metadata.ASCII_STRING_MARSHALLER);
  Metadata.Key<String> CALL_ID =
      Metadata.Key.of("call-id", Metadata.ASCII_STRING_MARSHALLER);

  static StatusRuntimeException wrapException(Throwable t) {
    return wrapException(t, -1);
  }

  static StatusRuntimeException wrapException(Throwable t, long callId) {
    t = JavaUtils.unwrapCompletionException(t);

    Metadata trailers = new Metadata();
    trailers.put(EXCEPTION_TYPE_KEY, t.getClass().getCanonicalName());
    if (callId > 0) {
      trailers.put(CALL_ID, String.valueOf(callId));
    }
    return new StatusRuntimeException(
        Status.INTERNAL.withCause(t).withDescription(t.getMessage()), trailers);
  }

  static Throwable unwrapThrowable(Throwable t) {
    if (t instanceof StatusRuntimeException) {
      final IOException ioe = tryUnwrapException((StatusRuntimeException)t);
      if (ioe != null) {
        return ioe;
      }
    }
    return t;
  }

  static IOException unwrapException(StatusRuntimeException se) {
    final IOException ioe = tryUnwrapException(se);
    return ioe != null? ioe: new IOException(se);
  }

  static IOException tryUnwrapException(StatusRuntimeException se) {
    final Metadata trailers = se.getTrailers();
    final Status status = se.getStatus();
    if (trailers != null && status != null) {
      final String className = trailers.get(EXCEPTION_TYPE_KEY);
      if (className != null) {
        try {
          Class<?> clazz = Class.forName(className);
          final Exception unwrapped = ReflectionUtils.instantiateException(
              clazz.asSubclass(Exception.class), status.getDescription(), se);
          return IOUtils.asIOException(unwrapped);
        } catch (Exception e) {
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

  static IOException unwrapIOException(Throwable t) {
    final IOException e;
    if (t instanceof StatusRuntimeException) {
      e = GrpcUtil.unwrapException((StatusRuntimeException) t);
    } else {
      e = IOUtils.asIOException(t);
    }
    return e;
  }

  static <REPLY extends RaftClientReply, REPLY_PROTO> void asyncCall(
      StreamObserver<REPLY_PROTO> responseObserver,
      CheckedSupplier<CompletableFuture<REPLY>, IOException> supplier,
      Function<REPLY, REPLY_PROTO> toProto) {
    try {
      supplier.get().whenCompleteAsync((reply, exception) -> {
        if (exception != null) {
          responseObserver.onError(GrpcUtil.wrapException(exception));
        } else {
          responseObserver.onNext(toProto.apply(reply));
          responseObserver.onCompleted();
        }
      });
    } catch (Exception e) {
      responseObserver.onError(GrpcUtil.wrapException(e));
    }
  }

  static void warn(Logger log, Supplier<String> message, Throwable t) {
    LogUtils.warn(log, message, unwrapThrowable(t), StatusRuntimeException.class, ServerNotReadyException.class);
  }
}
