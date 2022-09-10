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
package org.apache.ratis.grpc.metrics;

import org.apache.ratis.metrics.LongCounter;
import org.apache.ratis.metrics.MetricRegistryInfo;
import org.apache.ratis.metrics.RatisMetrics;

import java.util.Map;

public class MessageMetrics extends RatisMetrics {
  public static final String GRPC_MESSAGE_METRICS = "%s_message_metrics";
  public static final String GRPC_MESSAGE_METRICS_DESC = "Outbound/Inbound message counters";

  private enum Type {
    STARTED("_started_total"),
    COMPLETED("_completed_total"),
    RECEIVED("_received_executed");

    private final String suffix;

    Type(String suffix) {
      this.suffix = suffix;
    }

    String getSuffix() {
      return suffix;
    }
  }

  private final Map<Type, Map<String, LongCounter>> types;

  public MessageMetrics(String endpointId, String endpointType) {
    super(create(
        new MetricRegistryInfo(endpointId,
            RATIS_APPLICATION_NAME_METRICS,
            String.format(GRPC_MESSAGE_METRICS, endpointType),
            GRPC_MESSAGE_METRICS_DESC)
    ));

    this.types = newCounterMaps(Type.class);
  }

  private void inc(Type t, String rpcType) {
    types.get(t)
        .computeIfAbsent(rpcType, key -> getRegistry().counter(key + t.getSuffix()))
        .inc();
  }

  /**
   * Increments the count of RPCs that are started.
   * Both client and server use this.
   */
  public void rpcStarted(String rpcType){
    inc(Type.STARTED, rpcType);
  }

  /**
   * Increments the count of RPCs that were started and got completed.
   * Both client and server use this.
   */
  public void rpcCompleted(String rpcType){
    inc(Type.COMPLETED, rpcType);
  }

  /**
   * Increments the count of RPCs received on the server.
   */
  public void rpcReceived(String rpcType){
    inc(Type.RECEIVED, rpcType);
  }
}
