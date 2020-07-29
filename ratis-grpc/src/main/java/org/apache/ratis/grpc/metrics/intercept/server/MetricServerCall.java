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
