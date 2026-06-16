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
package org.apache.ratis.trace;

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.trace.otel.OTelTraceProvider;

import java.util.concurrent.atomic.AtomicReference;

/** Common tracing utilities shared by {@link TraceClient} and {@link TraceServer}. */
public final class TraceUtils {

  private static final AtomicReference<TraceProvider> PROVIDER =
      new AtomicReference<>(NoOpTraceProvider.INSTANCE);

  private TraceUtils() {
  }

  /**
   * Initializes tracing from configuration when tracing is enabled, or clears it when disabled.
   * Call from RaftServer and RaftClient construction so tracing follows {@link TraceConfigKeys}.
   *
   * @param properties raft configuration; tracing is on when {@link TraceConfigKeys#enabled} is true
   */
  public static void setTracerWhenEnabled(RaftProperties properties) {
    setTracerWhenEnabled(TraceConfigKeys.enabled(properties));
  }

  /**
   * Enables or disables tracing without reading {@link RaftProperties}. Intended for tests and
   * simple toggles; production code should prefer {@link #setTracerWhenEnabled(RaftProperties)}.
   *
   * @param enabled when true, enables the OpenTelemetry provider; when false, clears it
   */
  public static void setTracerWhenEnabled(boolean enabled) {
    PROVIDER.set(enabled ? newOpenTelemetryTraceProvider() : NoOpTraceProvider.INSTANCE);
  }

  public static boolean isEnabled() {
    return !(getProvider() instanceof NoOpTraceProvider);
  }

  static TraceProvider getProvider() {
    return PROVIDER.get();
  }

  private static TraceProvider newOpenTelemetryTraceProvider() {
    try {
      return new OTelTraceProvider();
    } catch (Throwable e) {
      throw new IllegalStateException("Failed to create OTelTraceProvider.", e);
    }
  }
}
