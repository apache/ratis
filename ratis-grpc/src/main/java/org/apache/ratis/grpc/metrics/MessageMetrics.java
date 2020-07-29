package org.apache.ratis.grpc.metrics;

import org.apache.ratis.grpc.server.GrpcService;
import org.apache.ratis.metrics.MetricRegistryInfo;
import org.apache.ratis.metrics.RatisMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageMetrics extends RatisMetrics {
  static final Logger LOG = LoggerFactory.getLogger(MessageMetrics.class);
  public static final String GRPC_MESSAGE_METRICS = "%s_message_metrics";
  public static final String GRPC_MESSAGE_METRICS_DESC = "Outbound/Inbound message counters";

  public MessageMetrics(String endpointId, String endpointType) {
    this.registry = create(
        new MetricRegistryInfo(endpointId,
            RATIS_APPLICATION_NAME_METRICS,
            String.format(GRPC_MESSAGE_METRICS, endpointType),
            GRPC_MESSAGE_METRICS_DESC)
    );
  }

  public void rpcStarted(String rpcType){
    LOG.info("Started rpc of type {}", rpcType);
    registry.counter(rpcType + "_started_total").inc();
  }

  public void rpcCompleted(String rpcType){
    LOG.info("Completed rpc of type {}", rpcType);
    registry.counter(rpcType + "_completed_total").inc();
  }

  public void rpcHandled(String rpcType){
    LOG.info("Executed rpc of type {}", rpcType);
    registry.counter(rpcType + "_received_handled").inc();
  }

}
