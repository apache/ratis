/**
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

import org.apache.ratis.shaded.io.grpc.Metadata;
import org.apache.ratis.shaded.io.grpc.Status;
import org.apache.ratis.shaded.io.grpc.StatusRuntimeException;
import org.apache.ratis.util.IOUtils;
import org.apache.ratis.util.ReflectionUtils;
import org.apache.ratis.util.StringUtils;

import java.io.IOException;

public class RaftGrpcUtil {
  public static final Metadata.Key<String> EXCEPTION_TYPE_KEY =
      Metadata.Key.of("exception-type", Metadata.ASCII_STRING_MARSHALLER);

  public static StatusRuntimeException wrapException(Throwable t) {
    Metadata trailers = new Metadata();
    trailers.put(EXCEPTION_TYPE_KEY, t.getClass().getCanonicalName());
    return new StatusRuntimeException(
        Status.INTERNAL.withDescription(StringUtils.stringifyException(t)),
        trailers);
  }

  public static IOException unwrapException(StatusRuntimeException se) {
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
          return new IOException(se);
        }
      }
    }
    return new IOException(se);
  }

  public static IOException unwrapIOException(Throwable t) {
    final IOException e;
    if (t instanceof StatusRuntimeException) {
      e = RaftGrpcUtil.unwrapException((StatusRuntimeException) t);
    } else {
      e = IOUtils.asIOException(t);
    }
    return e;
  }

}
