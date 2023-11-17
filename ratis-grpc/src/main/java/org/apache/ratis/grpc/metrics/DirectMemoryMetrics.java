/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ratis.grpc.metrics;

import com.codahale.metrics.Gauge;
import org.apache.ratis.metrics.MetricRegistryInfo;
import org.apache.ratis.metrics.RatisMetricRegistry;
import org.apache.ratis.metrics.RatisMetrics;

public class DirectMemoryMetrics extends RatisMetrics {
  private static final String RATIS_GRPC_METRICS_APP_NAME = "ratis_grpc";
  private static final String RATIS_GRPC_METRICS_COMP_NAME = "direct_memory_disposer";
  private static final String RATIS_GRPC_METRICS_DESC = "Metrics for Ratis Grpc direct memory management";

  public DirectMemoryMetrics() {
    registry = getMetricRegistryForGrpcServer();
  }

  private RatisMetricRegistry getMetricRegistryForGrpcServer() {
    return create(new MetricRegistryInfo("mem",
        RATIS_GRPC_METRICS_APP_NAME,
        RATIS_GRPC_METRICS_COMP_NAME, RATIS_GRPC_METRICS_DESC));
  }


  public void onBufferRegistered(long size) {
    registry.counter("mem_registered_bytes").inc(size);
  }

  public void onBufferDisposed(long size) {
    registry.counter("mem_disposed_bytes").inc(size);
  }

  public void watchPendingMemoryBytes(Gauge<Long> pendingMemBytes) {
    registry.gauge("mem_pending_bytes", () -> pendingMemBytes);
  }

  public void watchPendingBuffes(Gauge<Integer> pendingBuffers) {
    registry.gauge("pending_buffers", () -> pendingBuffers);
  }

}
