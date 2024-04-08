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
package org.apache.ratis.grpc.util;

import org.apache.ratis.thirdparty.io.grpc.CallOptions;
import org.apache.ratis.thirdparty.io.grpc.Channel;
import org.apache.ratis.thirdparty.io.grpc.ClientCall;
import org.apache.ratis.thirdparty.io.grpc.ClientInterceptor;
import org.apache.ratis.thirdparty.io.grpc.ForwardingClientCall;
import org.apache.ratis.thirdparty.io.grpc.ForwardingClientCallListener;
import org.apache.ratis.thirdparty.io.grpc.Metadata;
import org.apache.ratis.thirdparty.io.grpc.MethodDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

/**
 * Invoke the given notifier when receiving a response.
 */
public class ResponseNotifyClientInterceptor implements ClientInterceptor {
  public static final Logger LOG = LoggerFactory.getLogger(ResponseNotifyClientInterceptor.class);

  private final Consumer<Object> notifier;

  public ResponseNotifyClientInterceptor(Consumer<Object> notifier) {
    this.notifier = notifier;
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
    LOG.debug("callOptions {}", callOptions);
    return new Call<>(next.newCall(method, callOptions));
  }

  private final class Call<ReqT, RespT>
      extends ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT> {

    private Call(ClientCall<ReqT, RespT> delegate) {
      super(delegate);
    }

    @Override
    public void start(Listener<RespT> responseListener, Metadata headers) {
      LOG.debug("start {}", headers);
      super.start(new ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT>(responseListener) {
        @Override
        public void onMessage(RespT message) {
          LOG.debug("onMessage {}", message);
          notifier.accept(message);
          super.onMessage(message);
        }
      }, headers);
    }
  }
}
