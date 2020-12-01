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
package org.apache.ratis.client.impl;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import org.apache.ratis.client.AsyncRpcApi;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.proto.RaftProtos.ReplicationLevel;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftPeerId;

/** Async api implementations. */
class AsyncImpl implements AsyncRpcApi {
  private final RaftClientImpl client;

  AsyncImpl(RaftClientImpl client) {
    this.client = Objects.requireNonNull(client, "client == null");
  }

  CompletableFuture<RaftClientReply> send(
      RaftClientRequest.Type type, Message message, RaftPeerId server) {
    return client.getOrderedAsync().send(type, message, server);
  }

  @Override
  public CompletableFuture<RaftClientReply> send(Message message) {
    return send(RaftClientRequest.writeRequestType(), message, null);
  }

  @Override
  public CompletableFuture<RaftClientReply> sendReadOnly(Message message) {
    return send(RaftClientRequest.readRequestType(), message, null);
  }

  @Override
  public CompletableFuture<RaftClientReply> sendStaleRead(Message message, long minIndex, RaftPeerId server) {
    return send(RaftClientRequest.staleReadRequestType(minIndex), message, server);
  }

  @Override
  public CompletableFuture<RaftClientReply> watch(long index, ReplicationLevel replication) {
    return UnorderedAsync.send(RaftClientRequest.watchRequestType(index, replication), client);
  }

  @Override
  public CompletableFuture<RaftClientReply> sendForward(RaftClientRequest request) {
    final RaftProtos.RaftClientRequestProto proto = ClientProtoUtils.toRaftClientRequestProto(request);
    return send(RaftClientRequest.forwardRequestType(), Message.valueOf(proto.toByteString()), null);
  }
}
