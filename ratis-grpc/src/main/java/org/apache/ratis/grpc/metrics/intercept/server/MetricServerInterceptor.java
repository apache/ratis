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
import org.apache.ratis.thirdparty.io.grpc.MethodDescriptor;
import org.apache.ratis.thirdparty.io.grpc.ServerCall;
import org.apache.ratis.thirdparty.io.grpc.ServerCallHandler;
import org.apache.ratis.grpc.metrics.MessageMetrics;
import org.apache.ratis.thirdparty.io.grpc.ServerInterceptor;

public class MetricServerInterceptor implements ServerInterceptor {
  private final String identifier;
  private final MessageMetrics metrics;

  public MetricServerInterceptor(String identifier){
    this.identifier = identifier;
    this.metrics = new MessageMetrics(identifier, "server");
  }

  private String getMethodMetricPrefix(MethodDescriptor<?, ?> method){
    String serviceName = MethodDescriptor.extractFullServiceName(method.getFullMethodName());
    String methodName = method.getFullMethodName().substring(serviceName.length() + 1);
    return identifier + "_" + serviceName + "_" + methodName;
  }

  @Override
  public <R, S> ServerCall.Listener<R> interceptCall(
      ServerCall<R, S> call,
      Metadata requestHeaders,
      ServerCallHandler<R, S> next) {
    MethodDescriptor<R, S> method = call.getMethodDescriptor();
    String metricNamePrefix = getMethodMetricPrefix(method);
    ServerCall<R,S> monitoringCall = new MetricServerCall<>(call, metricNamePrefix, metrics);
    return new MetricServerCallListener<>(
        next.startCall(monitoringCall, requestHeaders), metricNamePrefix, metrics);
  }
}
