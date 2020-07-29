package org.apache.ratis.grpc.metrics.intercept.server;

import org.apache.ratis.thirdparty.io.grpc.ForwardingServerCallListener;
import org.apache.ratis.thirdparty.io.grpc.ServerCall;
import org.apache.ratis.grpc.metrics.MessageMetrics;

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
