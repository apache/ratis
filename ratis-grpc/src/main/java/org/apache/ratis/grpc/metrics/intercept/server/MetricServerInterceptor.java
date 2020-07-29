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
