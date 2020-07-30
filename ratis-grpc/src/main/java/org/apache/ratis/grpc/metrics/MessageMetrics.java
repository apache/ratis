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
package org.apache.ratis.grpc.metrics;

import org.apache.ratis.metrics.MetricRegistryInfo;
import org.apache.ratis.metrics.RatisMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageMetrics extends RatisMetrics {
  static final Logger LOG = LoggerFactory.getLogger(MessageMetrics.class);
  public static final String GRPC_MESSAGE_METRICS = "%s_message_metrics";
  public static final String GRPC_MESSAGE_METRICS_DESC = "Outbound/Inbound message counters";

  public MessageMetrics(String endpointId, String endpointType) {
    this.registry = create(
        new MetricRegistryInfo(endpointId,
            RATIS_APPLICATION_NAME_METRICS,
            String.format(GRPC_MESSAGE_METRICS, endpointType),
            GRPC_MESSAGE_METRICS_DESC)
    );
  }

  /**
   * Increments the count of RPCs that are started.
   * Both client and server use this.
   * @param rpcType
   */
  public void rpcStarted(String rpcType){
    registry.counter(rpcType + "_started_total").inc();
  }

  /**
   * Increments the count of RPCs that were started and got completed.
   * Both client and server use this.
   * @param rpcType
   */
  public void rpcCompleted(String rpcType){
    registry.counter(rpcType + "_completed_total").inc();
  }

  /**
   * increments the count of RPCs recived on the server.
   * @param rpcType
   */
  public void rpcReceived(String rpcType){
    registry.counter(rpcType + "_received_executed").inc();
  }

}
