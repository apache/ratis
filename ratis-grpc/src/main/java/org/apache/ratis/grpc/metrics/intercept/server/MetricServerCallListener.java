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

package org.apache.ratis.grpc.metrics.intercept.server;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.ratis.thirdparty.io.grpc.ForwardingServerCallListener;
import org.apache.ratis.thirdparty.io.grpc.ServerCall;
import org.apache.ratis.grpc.metrics.MessageMetrics;

@SuppressFBWarnings("URF_UNREAD_FIELD")
public class MetricServerCallListener<R> extends ForwardingServerCallListener<R> {
  private final ServerCall.Listener<R> delegate;
  private final String metricNamePrefix;
  private MessageMetrics metrics;

  MetricServerCallListener(
      ServerCall.Listener<R> delegate,
      String metricNamePrefix,
      MessageMetrics metrics
  ){
    this.delegate = delegate;
    this.metricNamePrefix = metricNamePrefix;
    this.metrics = metrics;
  }

  @Override
  protected ServerCall.Listener<R> delegate() {
    return delegate;
  }
}
