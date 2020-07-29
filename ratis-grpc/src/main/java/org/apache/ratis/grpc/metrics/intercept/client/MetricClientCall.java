package org.apache.ratis.grpc.metrics.intercept.client;

import org.apache.ratis.thirdparty.io.grpc.ClientCall;
import org.apache.ratis.thirdparty.io.grpc.ForwardingClientCall;
import org.apache.ratis.thirdparty.io.grpc.Metadata;
import org.apache.ratis.grpc.metrics.MessageMetrics;

public class MetricClientCall<R, S> extends ForwardingClientCall.SimpleForwardingClientCall<R, S> {
  private final String metricNamePrefix;
  private final MessageMetrics metrics;

  public MetricClientCall(ClientCall<R, S> delegate,
                          MessageMetrics metrics,
                          String metricName){
    super(delegate);
    this.metricNamePrefix = metricName;
    this.metrics = metrics;
  }

  @Override
  public void start(ClientCall.Listener<S> delegate, Metadata metadata) {
    metrics.rpcStarted(metricNamePrefix);
    super.start(new MetricClientCallListener<>(
        delegate, metrics, metricNamePrefix), metadata);
  }
}
