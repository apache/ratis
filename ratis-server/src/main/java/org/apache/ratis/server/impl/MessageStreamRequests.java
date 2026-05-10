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
package org.apache.ratis.server.impl;

import org.apache.ratis.proto.RaftProtos.MessageStreamRequestTypeProto;
import org.apache.ratis.protocol.ClientInvocationId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.exceptions.StreamException;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.ReferenceCountedObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

class MessageStreamRequests {
  public static final Logger LOG = LoggerFactory.getLogger(MessageStreamRequests.class);

  private static class PendingStream {
    private final ClientInvocationId key;
    private long nextId = -1;
    private ByteString bytes = ByteString.EMPTY;
    private final List<ReferenceCountedObject<RaftClientRequest>> pendingRefs = new LinkedList<>();

    PendingStream(ClientInvocationId key) {
      this.key = key;
    }

    synchronized CompletableFuture<ByteString> append(long messageId,
        ReferenceCountedObject<RaftClientRequest> requestRef) {
      if (nextId == -1) {
        nextId = messageId;
      } else if (messageId != nextId) {
        return JavaUtils.completeExceptionally(new StreamException(
            "Unexpected message id in " + key + ": messageId = " + messageId + " != nextId = " + nextId));
      }
      nextId++;
      final Message message = requestRef.retain().getMessage();
      pendingRefs.add(requestRef);
      bytes = bytes.concat(message.getContent());
      return CompletableFuture.completedFuture(bytes);
    }

    synchronized CompletableFuture<ReferenceCountedObject<RaftClientRequest>> getWriteRequest(long messageId,
        ReferenceCountedObject<RaftClientRequest> requestRef) {
      return append(messageId, requestRef)
          .thenApply(appended -> RaftClientRequest.toWriteRequest(requestRef.get(), () -> appended))
          .thenApply(request -> ReferenceCountedObject.delegateFrom(pendingRefs, request));
    }

    synchronized void clear() {
      pendingRefs.forEach(ReferenceCountedObject::release);
      pendingRefs.clear();
    }
  }

  static class StreamMap {
    private final Map<ClientInvocationId, PendingStream> map = new HashMap<>();

    synchronized PendingStream computeIfAbsent(ClientInvocationId key) {
      return map.computeIfAbsent(key, PendingStream::new);
    }

    synchronized PendingStream remove(ClientInvocationId key) {
      return map.remove(key);
    }

    synchronized void clear() {
      map.values().forEach(PendingStream::clear);
      map.clear();
    }
  }

  private final String name;
  private final StreamMap streams = new StreamMap();

  MessageStreamRequests(Object name) {
    this.name = name + "-" + JavaUtils.getClassSimpleName(getClass());
  }

  CompletableFuture<?> streamAsync(ReferenceCountedObject<RaftClientRequest> requestRef) {
    final RaftClientRequest request = requestRef.get();
    final MessageStreamRequestTypeProto stream = request.getType().getMessageStream();
    Preconditions.assertTrue(!stream.getEndOfRequest());
    final ClientInvocationId key = ClientInvocationId.valueOf(request.getClientId(), stream.getStreamId());
    final PendingStream pending = streams.computeIfAbsent(key);
    return pending.append(stream.getMessageId(), requestRef);
  }

  CompletableFuture<ReferenceCountedObject<RaftClientRequest>> streamEndOfRequestAsync(
      ReferenceCountedObject<RaftClientRequest> requestRef) {
    final RaftClientRequest request = requestRef.get();
    final MessageStreamRequestTypeProto stream = request.getType().getMessageStream();
    Preconditions.assertTrue(stream.getEndOfRequest());
    final ClientInvocationId key = ClientInvocationId.valueOf(request.getClientId(), stream.getStreamId());

    final PendingStream pending = streams.remove(key);
    if (pending == null) {
      return JavaUtils.completeExceptionally(new StreamException(name + ": " + key + " not found"));
    }
    return pending.getWriteRequest(stream.getMessageId(), requestRef);
  }

  void clear() {
    streams.clear();
  }
}
