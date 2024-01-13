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

import org.apache.ratis.metrics.LongCounter;
import org.apache.ratis.metrics.MetricRegistryInfo;
import org.apache.ratis.metrics.RatisMetricRegistry;
import org.apache.ratis.metrics.RatisMetrics;
import org.apache.ratis.thirdparty.com.google.protobuf.AbstractMessage;

public class ZeroCopyMetrics extends RatisMetrics {
  private static final String RATIS_GRPC_METRICS_APP_NAME = "ratis_grpc";
  private static final String RATIS_GRPC_METRICS_COMP_NAME = "zero_copy";
  private static final String RATIS_GRPC_METRICS_DESC = "Metrics for Ratis Grpc Zero copy";

  private final LongCounter zeroCopyMessages = getRegistry().counter("num_zero_copy_messages");
  private final LongCounter nonZeroCopyMessages = getRegistry().counter("num_non_zero_copy_messages");
  private final LongCounter releasedMessages = getRegistry().counter("num_released_messages");

  public ZeroCopyMetrics() {
    super(createRegistry());
  }

  private static RatisMetricRegistry createRegistry() {
    return create(new MetricRegistryInfo("",
        RATIS_GRPC_METRICS_APP_NAME,
        RATIS_GRPC_METRICS_COMP_NAME, RATIS_GRPC_METRICS_DESC));
  }


  public void onZeroCopyMessage(AbstractMessage ignored) {
    zeroCopyMessages.inc();
  }

  public void onNonZeroCopyMessage(AbstractMessage ignored) {
    nonZeroCopyMessages.inc();
  }

  public void onReleasedMessage(AbstractMessage ignored) {
    releasedMessages.inc();
  }

}