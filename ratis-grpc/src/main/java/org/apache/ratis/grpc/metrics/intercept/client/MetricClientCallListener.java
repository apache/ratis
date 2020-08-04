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
package org.apache.ratis.grpc.metrics.intercept.client;

import org.apache.ratis.thirdparty.io.grpc.ClientCall;
import org.apache.ratis.thirdparty.io.grpc.ForwardingClientCallListener;
import org.apache.ratis.thirdparty.io.grpc.Metadata;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.grpc.metrics.MessageMetrics;

public class MetricClientCallListener<S> extends ForwardingClientCallListener<S> {
  private final String metricNamePrefix;
  private final MessageMetrics metrics;
  private final ClientCall.Listener<S> delegate;

  MetricClientCallListener(ClientCall.Listener<S> delegate,
                           MessageMetrics metrics,
                           String metricNamePrefix){
    this.delegate = delegate;
    this.metricNamePrefix = metricNamePrefix;
    this.metrics = metrics;
  }

  @Override
  protected ClientCall.Listener<S> delegate() {
    return delegate;
  }

  @Override
  public void onClose(Status status, Metadata metadata) {
    metrics.rpcReceived(metricNamePrefix + "_" + status.getCode().toString());
    super.onClose(status, metadata);
  }
}
