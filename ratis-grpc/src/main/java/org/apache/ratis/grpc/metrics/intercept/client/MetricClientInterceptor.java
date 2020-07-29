package org.apache.ratis.grpc.metrics.intercept.client;

import org.apache.ratis.grpc.metrics.MessageMetrics;
import org.apache.ratis.thirdparty.io.grpc.*;

public class MetricClientInterceptor implements ClientInterceptor {
  private final String identifier;
  private final MessageMetrics metrics;

  public MetricClientInterceptor(String identifier){
    this.identifier = identifier;
    this.metrics = new MessageMetrics(identifier, "client");
  }

  private String getMethodMetricPrefix(MethodDescriptor<?, ?> method){
    String serviceName = MethodDescriptor.extractFullServiceName(method.getFullMethodName());
    String methodName = method.getFullMethodName().substring(serviceName.length() + 1);
    return identifier + "_" + serviceName + "_" + methodName;
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> methodDescriptor,
                                                             CallOptions callOptions,
                                                             Channel channel) {

    return new MetricClientCall<>(
        channel.newCall(methodDescriptor, callOptions),
        metrics,
        getMethodMetricPrefix(methodDescriptor)
    );
  }
}
