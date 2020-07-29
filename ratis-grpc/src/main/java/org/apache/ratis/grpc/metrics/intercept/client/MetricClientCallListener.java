package org.apache.ratis.grpc.metrics.intercept.client;

import org.apache.ratis.thirdparty.io.grpc.ClientCall;
import org.apache.ratis.thirdparty.io.grpc.ForwardingClientCallListener;
import org.apache.ratis.thirdparty.io.grpc.Metadata;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.grpc.metrics.MessageMetrics;

import java.time.Clock;

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
    metrics.rpcHandled(metricNamePrefix + "_" + status.getCode().toString());
    super.onClose(status, metadata);
  }
}
