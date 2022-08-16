/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ratis.netty.metrics;

import com.codahale.metrics.Timer;
import org.apache.ratis.metrics.MetricRegistryInfo;
import org.apache.ratis.metrics.RatisMetricRegistry;
import org.apache.ratis.metrics.RatisMetrics;

import java.util.Locale;

public class NettyServerStreamRpcMetrics extends RatisMetrics {
  private static final String METRICS_APP_NAME = "ratis_netty";
  private static final String METRICS_COMP_NAME = "stream_server";
  private static final String METRICS_DESC = "Metrics for Ratis Netty Stream Server";

  private static final String METRICS_LATENCY = "%s_latency";
  private static final String METRICS_SUCCESS = "%s_success_reply_count";
  private static final String METRICS_FAIL = "%s_fail_reply_count";
  private static final String METRICS_NUM_REQUESTS = "num_requests_%s";

  public enum RequestType {
    CHANNEL_READ, HEADER, LOCAL_WRITE, REMOTE_WRITE, STATE_MACHINE_STREAM, START_TRANSACTION;

    private final String numRequestsString;
    private final String successCountString;
    private final String failCountString;
    private final String latencyString;

    RequestType() {
      final String lower = name().toLowerCase(Locale.ENGLISH);
      this.numRequestsString = String.format(METRICS_NUM_REQUESTS, lower);
      this.successCountString = String.format(METRICS_SUCCESS, lower);
      this.failCountString = String.format(METRICS_FAIL, lower);
      this.latencyString = String.format(METRICS_LATENCY, lower);
    }

    String getNumRequestsString() {
      return numRequestsString;
    }
    String getSuccessCountString() {
      return successCountString;
    }
    String getFailCountString() {
      return failCountString;
    }
    String getLatencyString() {
      return latencyString;
    }
  }

  public static final class RequestContext {
    private final Timer.Context timerContext;

    private RequestContext(Timer.Context timerContext) {
      this.timerContext = timerContext;
    }

    Timer.Context getTimerContext() {
      return timerContext;
    }
  }

  public final class RequestMetrics {
    private final RequestType type;
    private final Timer timer;

    private RequestMetrics(RequestType type) {
      this.type = type;
      this.timer = getLatencyTimer(type);
    }

    public RequestContext start() {
      onRequestCreate(type);
      return new RequestContext(timer.time());
    }

    public void stop(RequestContext context, boolean success) {
      context.getTimerContext().stop();
      if (success) {
        onRequestSuccess(type);
      } else {
        onRequestFail(type);
      }
    }
  }

  public NettyServerStreamRpcMetrics(String serverId) {
    registry = getMetricRegistryForGrpcServer(serverId);
  }

  private RatisMetricRegistry getMetricRegistryForGrpcServer(String serverId) {
    return create(new MetricRegistryInfo(serverId,
        METRICS_APP_NAME, METRICS_COMP_NAME, METRICS_DESC));
  }

  public RequestMetrics newRequestMetrics(RequestType type) {
    return new RequestMetrics(type);
  }

  public Timer getLatencyTimer(RequestType type) {
    return registry.timer(type.getLatencyString());
  }

  public void onRequestCreate(RequestType type) {
    registry.counter(type.getNumRequestsString()).inc();
  }

  public void onRequestSuccess(RequestType type) {
    registry.counter(type.getSuccessCountString()).inc();
  }

  public void onRequestFail(RequestType type) {
    registry.counter(type.getFailCountString()).inc();
  }
}
