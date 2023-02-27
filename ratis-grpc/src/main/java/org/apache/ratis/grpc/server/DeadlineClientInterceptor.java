/*
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
package org.apache.ratis.grpc.server;

import org.apache.ratis.thirdparty.io.grpc.CallOptions;
import org.apache.ratis.thirdparty.io.grpc.Channel;
import org.apache.ratis.thirdparty.io.grpc.ClientCall;
import org.apache.ratis.thirdparty.io.grpc.ClientInterceptor;
import org.apache.ratis.thirdparty.io.grpc.ForwardingClientCall;
import org.apache.ratis.thirdparty.io.grpc.ForwardingClientCallListener;
import org.apache.ratis.thirdparty.io.grpc.Metadata;
import org.apache.ratis.thirdparty.io.grpc.MethodDescriptor;
import org.apache.ratis.thirdparty.io.grpc.internal.SharedResourceHolder;
import org.apache.ratis.util.TimeDuration;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import static org.apache.ratis.thirdparty.io.grpc.internal.GrpcUtil.TIMER_SERVICE;

/**
 * Intercepts the messages and resets the deadline for each streaming call.
 */
public class DeadlineClientInterceptor implements ClientInterceptor {
  private final TimeDuration timeout;
  private static final ScheduledExecutorService Timer = SharedResourceHolder.get(TIMER_SERVICE);
  public DeadlineClientInterceptor(TimeDuration timeout) {
    this.timeout = timeout;
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
    return new CallWithDeadline<>(next.newCall(method, callOptions), timeout);
  }

  private static final class CallWithDeadline<ReqT, RespT>
    extends ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT> {
    private final ScheduledFuture<?> timeoutFuture;

    private CallWithDeadline(ClientCall<ReqT, RespT> delegate, TimeDuration timeout) {
      super(delegate);
      timeoutFuture = Timer.schedule(() -> cancel("streaming call timeouts", null),
          timeout.getDuration(), timeout.getUnit());
    }

    @Override
    public void start(Listener<RespT> responseListener, Metadata headers) {
      super.start(new ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT>(responseListener) {
        @Override
        public void onHeaders(Metadata headers) {
          // on receiving response headers, we should cancel the deadline timer
          timeoutFuture.cancel(false);
          super.onHeaders(headers);
        }
      }, headers);
    }
  }
}
