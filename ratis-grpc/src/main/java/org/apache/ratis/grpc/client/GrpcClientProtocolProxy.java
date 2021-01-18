/**
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
package org.apache.ratis.grpc.client;

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcTlsConfig;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.apache.ratis.proto.RaftProtos.RaftClientReplyProto;
import org.apache.ratis.proto.RaftProtos.RaftClientRequestProto;
import org.apache.ratis.protocol.RaftPeer;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Function;

public class GrpcClientProtocolProxy implements Closeable {
  private final GrpcClientProtocolClient proxy;
  private final Function<RaftPeer, CloseableStreamObserver> responseHandlerCreation;
  private RpcSession currentSession;

  public GrpcClientProtocolProxy(ClientId clientId, RaftPeer target,
      Function<RaftPeer, CloseableStreamObserver> responseHandlerCreation,
      RaftProperties properties, GrpcTlsConfig tlsConfig) {
    proxy = new GrpcClientProtocolClient(clientId, target, properties, tlsConfig, tlsConfig);
    this.responseHandlerCreation = responseHandlerCreation;
  }

  @Override
  public void close() throws IOException {
    closeCurrentSession();
    proxy.close();
  }

  @Override
  public String toString() {
    return "ProxyTo:" + proxy.getTarget();
  }

  public void closeCurrentSession() {
    if (currentSession != null) {
      currentSession.close();
      currentSession = null;
    }
  }

  public void onNext(RaftClientRequestProto request) {
    if (currentSession == null) {
      currentSession = new RpcSession(
          responseHandlerCreation.apply(proxy.getTarget()));
    }
    currentSession.requestObserver.onNext(request);
  }

  public void onError() {
    if (currentSession != null) {
      currentSession.onError();
    }
  }

  public interface CloseableStreamObserver
      extends StreamObserver<RaftClientReplyProto>, Closeable {
  }

  class RpcSession implements Closeable {
    private final StreamObserver<RaftClientRequestProto> requestObserver;
    private final CloseableStreamObserver responseHandler;
    private boolean hasError = false;

    RpcSession(CloseableStreamObserver responseHandler) {
      this.responseHandler = responseHandler;
      this.requestObserver = proxy.ordered(responseHandler);
    }

    void onError() {
      hasError = true;
    }

    @Override
    public void close() {
      if (!hasError) {
        try {
          requestObserver.onCompleted();
        } catch (Exception ignored) {
        }
      }
      try {
        responseHandler.close();
      } catch (IOException ignored) {
      }
    }
  }
}
