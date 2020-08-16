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

import org.apache.ratis.thirdparty.io.grpc.Metadata;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.thirdparty.io.grpc.ForwardingServerCall;
import org.apache.ratis.thirdparty.io.grpc.ServerCall;
import org.apache.ratis.grpc.metrics.MessageMetrics;

class MetricServerCall<R,S> extends ForwardingServerCall.SimpleForwardingServerCall<R,S> {
  private final MessageMetrics metrics;
  private final String metricNamPrefix;
  private final ServerCall<R,S> delegate;

  MetricServerCall(ServerCall<R,S> delegate,
                       String metricNamPrefix,
                       MessageMetrics metrics){
    super(delegate);
    this.delegate = delegate;
    this.metricNamPrefix = metricNamPrefix;
    this.metrics = metrics;

    metrics.rpcStarted(metricNamPrefix);
  }

  @Override
  public void close(Status status, Metadata responseHeaders) {
    metrics.rpcCompleted(metricNamPrefix + "_" + status.getCode().toString());
    super.close(status, responseHeaders);
  }

}
