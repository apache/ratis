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

import org.apache.ratis.client.impl.ClientProtoUtils;
import org.apache.ratis.client.impl.RaftClientRpcWithProxy;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.RaftGrpcUtil;
import org.apache.ratis.protocol.*;
import org.apache.ratis.shaded.io.grpc.stub.StreamObserver;
import org.apache.ratis.shaded.proto.RaftProtos;
import org.apache.ratis.shaded.proto.RaftProtos.RaftClientReplyProto;
import org.apache.ratis.shaded.proto.RaftProtos.RaftClientRequestProto;
import org.apache.ratis.shaded.proto.RaftProtos.SetConfigurationRequestProto;
import org.apache.ratis.util.IOUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.PeerProxyMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.ratis.client.impl.ClientProtoUtils.*;

public class GrpcClientRpc extends RaftClientRpcWithProxy<RaftClientProtocolClient> {
  public static final Logger LOG = LoggerFactory.getLogger(GrpcClientRpc.class);

  private final ClientId clientId;
  private final int maxMessageSize;

  public GrpcClientRpc(ClientId clientId, RaftProperties properties) {
    super(new PeerProxyMap<>(clientId.toString(), p -> new RaftClientProtocolClient(clientId, p)));
    this.clientId = clientId;
    maxMessageSize = GrpcConfigKeys.messageSizeMax(properties).getSizeInt();
  }

  @Override
  public CompletableFuture<RaftClientReply> sendRequestAsync(
      RaftClientRequest request) {
    final RaftPeerId serverId = request.getServerId();
    try {
      return sendRequestAsync(request, getProxies().getProxy(serverId));
    } catch (IOException e) {
      return JavaUtils.completeExceptionally(e);
    }
  }

  @Override
  public RaftClientReply sendRequest(RaftClientRequest request)
      throws IOException {
    final RaftPeerId serverId = request.getServerId();
    final RaftClientProtocolClient proxy = getProxies().getProxy(serverId);
    if (request instanceof ReinitializeRequest) {
      RaftProtos.ReinitializeRequestProto proto =
          toReinitializeRequestProto((ReinitializeRequest) request);
      return toRaftClientReply(proxy.reinitialize(proto));
    } else if (request instanceof SetConfigurationRequest) {
      SetConfigurationRequestProto setConf =
          toSetConfigurationRequestProto((SetConfigurationRequest) request);
      return toRaftClientReply(proxy.setConfiguration(setConf));
    } else if (request instanceof ServerInformatonRequest){
      RaftProtos.ServerInformationRequestProto proto =
          toServerInformationRequestProto((ServerInformatonRequest) request);
      return ClientProtoUtils.toServerInformationReply(
          proxy.serverInformation(proto));
    } else {
      final CompletableFuture<RaftClientReply> f = sendRequestAsync(request, proxy);
      // TODO: timeout support
      try {
        return f.get();
      } catch (InterruptedException e) {
        throw new InterruptedIOException(
            "Interrupted while waiting for response of request " + request);
      } catch (ExecutionException e) {
        throw IOUtils.toIOException(e);
      }
    }
  }

  private CompletableFuture<RaftClientReply> sendRequestAsync(
      RaftClientRequest request, RaftClientProtocolClient proxy) throws IOException {
    final RaftClientRequestProto requestProto =
        toRaftClientRequestProto(request);
    final CompletableFuture<RaftClientReplyProto> replyFuture =
        new CompletableFuture<>();
    final StreamObserver<RaftClientRequestProto> requestObserver =
        proxy.append(new StreamObserver<RaftClientReplyProto>() {
          @Override
          public void onNext(RaftClientReplyProto value) {
            replyFuture.complete(value);
          }

          @Override
          public void onError(Throwable t) {
            replyFuture.completeExceptionally(RaftGrpcUtil.unwrapIOException(t));
          }

          @Override
          public void onCompleted() {
            if (!replyFuture.isDone()) {
              replyFuture.completeExceptionally(
                  new IOException(clientId + ": Stream completed but no reply for request " + request));
            }
          }
        });
    requestObserver.onNext(requestProto);
    requestObserver.onCompleted();

    return replyFuture.thenApply(replyProto -> toRaftClientReply(replyProto));
  }

  RaftClientRequestProto toRaftClientRequestProto(RaftClientRequest request) throws IOException {
    final RaftClientRequestProto proto = ClientProtoUtils.toRaftClientRequestProto(request);
    if (proto.getSerializedSize() > maxMessageSize) {
      throw new IOException(clientId + ": Message size:" + proto.getSerializedSize()
          + " exceeds maximum:" + maxMessageSize);
    }
    return proto;
  }
}
