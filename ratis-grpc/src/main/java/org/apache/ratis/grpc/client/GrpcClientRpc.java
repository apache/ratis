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

import org.apache.ratis.client.RaftClientRpc;
import org.apache.ratis.grpc.RaftGrpcUtil;
import org.apache.ratis.protocol.*;
import org.apache.ratis.shaded.io.grpc.StatusRuntimeException;
import org.apache.ratis.shaded.io.grpc.stub.StreamObserver;
import org.apache.ratis.shaded.proto.RaftProtos.RaftClientReplyProto;
import org.apache.ratis.shaded.proto.RaftProtos.RaftClientRequestProto;
import org.apache.ratis.shaded.proto.RaftProtos.SetConfigurationRequestProto;
import org.apache.ratis.util.IOUtils;
import org.apache.ratis.util.PeerProxyMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.ratis.client.impl.ClientProtoUtils.*;

public class GrpcClientRpc implements RaftClientRpc {
  public static final Logger LOG = LoggerFactory.getLogger(GrpcClientRpc.class);

  private final PeerProxyMap<RaftClientProtocolClient> proxies
      = new PeerProxyMap<>(RaftClientProtocolClient::new);

  @Override
  public RaftClientReply sendRequest(RaftClientRequest request)
      throws IOException {
    final RaftPeerId serverId = request.getServerId();
    final RaftClientProtocolClient proxy = proxies.getProxy(serverId);
    if (request instanceof SetConfigurationRequest) {
      SetConfigurationRequestProto setConf =
          toSetConfigurationRequestProto((SetConfigurationRequest) request);
      return toRaftClientReply(proxy.setConfiguration(setConf));
    } else {
      RaftClientRequestProto requestProto = toRaftClientRequestProto(request);
      CompletableFuture<RaftClientReplyProto> replyFuture =
          new CompletableFuture<>();
      final StreamObserver<RaftClientRequestProto> requestObserver =
          proxy.append(new StreamObserver<RaftClientReplyProto>() {
            @Override
            public void onNext(RaftClientReplyProto value) {
              replyFuture.complete(value);
            }

            @Override
            public void onError(Throwable t) {
              // This implementation is used as RaftClientRpc. Retry
              // logic on Exception is in RaftClient.
              final IOException e;
              if (t instanceof StatusRuntimeException) {
                e = RaftGrpcUtil.unwrapException((StatusRuntimeException) t);
              } else {
                e = IOUtils.asIOException(t);
              }
              replyFuture.completeExceptionally(e);
            }

            @Override
            public void onCompleted() {
              if (!replyFuture.isDone()) {
                replyFuture.completeExceptionally(
                    new IOException("No reply for request " + request));
              }
            }
          });
      requestObserver.onNext(requestProto);
      requestObserver.onCompleted();

      // TODO: timeout support
      try {
        return toRaftClientReply(replyFuture.get());
      } catch (InterruptedException e) {
        throw new InterruptedIOException(
            "Interrupted while waiting for response of request " + request);
      } catch (ExecutionException e) {
        throw IOUtils.toIOException(e);
      }
    }
  }

  @Override
  public void addServers(Iterable<RaftPeer> servers) {
    proxies.addPeers(servers);
  }

  @Override
  public void close() throws IOException {
    proxies.close();
  }
}
