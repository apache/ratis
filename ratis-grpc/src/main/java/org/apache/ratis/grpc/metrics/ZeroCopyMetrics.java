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

import java.util.function.Supplier;

import org.apache.ratis.grpc.util.ZeroCopyMessageMarshaller.Metrics;
import org.apache.ratis.metrics.LongCounter;
import org.apache.ratis.metrics.MetricRegistryInfo;
import org.apache.ratis.metrics.RatisMetricRegistry;
import org.apache.ratis.metrics.RatisMetrics;
import org.apache.ratis.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.ratis.thirdparty.com.google.protobuf.AbstractMessage;

public class ZeroCopyMetrics extends RatisMetrics {
  private static final String RATIS_GRPC_METRICS_APP_NAME = "ratis_grpc";
  private static final String RATIS_GRPC_METRICS_COMP_NAME = "zero_copy";
  private static final String RATIS_GRPC_METRICS_DESC = "Metrics for Ratis Grpc Zero copy";

  private final LongCounter zeroCopyMessages = getRegistry().counter("num_zero_copy_messages");
  private final LongCounter nonZeroCopyMessages = getRegistry().counter("num_non_zero_copy_messages");
  private final LongCounter releasedMessages = getRegistry().counter("num_released_messages");

  // Per-message-type zero-copy counters.
  private final LongCounter zeroCopyAppendEntries = getRegistry().counter("num_zero_copy_append_entries");
  private final LongCounter zeroCopyInstallSnapshot = getRegistry().counter("num_zero_copy_install_snapshot");
  private final LongCounter zeroCopyClientRequest = getRegistry().counter("num_zero_copy_client_request");

  // Aggregated savings and parse time (nanos) for zero-copy path.
  private final LongCounter bytesSavedByZeroCopy = getRegistry().counter("bytes_saved_by_zero_copy");
  private final LongCounter zeroCopyParseTimeNanos = getRegistry().counter("zero_copy_parse_time_nanos");

  // Reason counters for zero-copy fallback.
  private final LongCounter fallbackNotKnownLength = getRegistry().counter("zero_copy_fallback_not_known_length");
  private final LongCounter fallbackNotDetachable = getRegistry().counter("zero_copy_fallback_not_detachable");
  private final LongCounter fallbackNotByteBuffer = getRegistry().counter("zero_copy_fallback_not_byte_buffer");

  public ZeroCopyMetrics() {
    super(createRegistry());
  }

  private static RatisMetricRegistry createRegistry() {
    return create(new MetricRegistryInfo("",
        RATIS_GRPC_METRICS_APP_NAME,
        RATIS_GRPC_METRICS_COMP_NAME, RATIS_GRPC_METRICS_DESC));
  }

  public void addUnreleased(String name, Supplier<Integer> unreleased) {
    getRegistry().gauge(name +  "_num_unreleased_messages", () -> unreleased);
  }


  public void onZeroCopyMessage(AbstractMessage ignored) {
    zeroCopyMessages.inc();
  }

  public void onZeroCopyAppendEntries(AbstractMessage ignored) {
    onZeroCopyMessage(ignored);
    zeroCopyAppendEntries.inc();
  }

  public void onZeroCopyInstallSnapshot(AbstractMessage ignored) {
    onZeroCopyMessage(ignored);
    zeroCopyInstallSnapshot.inc();
  }

  public void onZeroCopyClientRequest(AbstractMessage ignored) {
    onZeroCopyMessage(ignored);
    zeroCopyClientRequest.inc();
  }

  public void onNonZeroCopyMessage(AbstractMessage ignored) {
    nonZeroCopyMessages.inc();
  }

  public void onReleasedMessage(AbstractMessage ignored) {
    releasedMessages.inc();
  }

  public ZeroCopyMessageMarshallerMetrics newMarshallerMetrics() {
    return new ZeroCopyMessageMarshallerMetrics();
  }

  // Adapter used by ZeroCopyMessageMarshaller to report parse stats and fallback reasons.
  public class ZeroCopyMessageMarshallerMetrics implements Metrics {
    @Override
    public void onZeroCopyParse(long bytesSaved, long parseTimeNanos) {
      bytesSavedByZeroCopy.inc(bytesSaved);
      zeroCopyParseTimeNanos.inc(parseTimeNanos);
    }

    @Override
    public void onFallbackNotKnownLength() {
      fallbackNotKnownLength.inc();
    }

    @Override
    public void onFallbackNotDetachable() {
      fallbackNotDetachable.inc();
    }

    @Override
    public void onFallbackNotByteBuffer() {
      fallbackNotByteBuffer.inc();
    }
  }

  @VisibleForTesting
  public long zeroCopyMessages() {
    return zeroCopyMessages.getCount();
  }

  @VisibleForTesting
  public long nonZeroCopyMessages() {
    return nonZeroCopyMessages.getCount();
  }

  @VisibleForTesting
  public long releasedMessages() {
    return releasedMessages.getCount();
  }
}
