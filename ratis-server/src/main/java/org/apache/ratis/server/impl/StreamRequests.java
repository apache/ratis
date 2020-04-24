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

import org.apache.ratis.proto.RaftProtos.StreamRequestTypeProto;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.exceptions.StreamException;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

class StreamRequests {
  public static final Logger LOG = LoggerFactory.getLogger(StreamRequests.class);

  private static class Key {
    private final ClientId clientId;
    private final long streamId;

    Key(ClientId clientId, long streamId) {
      this.clientId = clientId;
      this.streamId = streamId;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      } else if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      final Key that = (Key) obj;
      return this.streamId == that.streamId && this.clientId.equals(that.clientId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(clientId, streamId);
    }

    @Override
    public String toString() {
      return "Stream" + streamId + "@" + clientId;
    }
  }

  private static class PendingStream {
    private final Key key;
    private long nextId = 0;
    private ByteString bytes = ByteString.EMPTY;

    PendingStream(Key key) {
      this.key = key;
    }

    synchronized CompletableFuture<ByteString> append(long messageId, Message message) {
      if (messageId != nextId) {
        return JavaUtils.completeExceptionally(new StreamException(
            "Unexpected message id in " + key + ": messageId = " + messageId + " != nextId = " + nextId));
      }
      nextId++;
      bytes = bytes.concat(message.getContent());
      return CompletableFuture.completedFuture(bytes);
    }

    synchronized CompletableFuture<ByteString> getBytes(long messageId, Message message) {
      return append(messageId, message);
    }
  }

  static class StreamMap {
    private final ConcurrentMap<Key, PendingStream> map = new ConcurrentHashMap<>();

    PendingStream add(Key key) {
      final PendingStream pending = new PendingStream(key);
      final PendingStream previous = map.put(key, pending);
      Preconditions.assertNull(previous, "previous");
      return pending;
    }

    PendingStream get(Key key) {
      return map.get(key);
    }

    PendingStream remove(Key key) {
      return map.remove(key);
    }

    void clear() {
      map.clear();
    }
  }

  private final String name;
  private final StreamMap streams = new StreamMap();

  StreamRequests(Object name) {
    this.name = name + "-" + getClass().getSimpleName();
  }

  CompletableFuture<?> streamAsync(RaftClientRequest request) {
    final StreamRequestTypeProto stream = request.getType().getStream();
    Preconditions.assertTrue(!stream.getClose());
    final Key key = new Key(request.getClientId(), stream.getStreamId());
    final PendingStream pending = stream.getMessageId() == 0? streams.add(key): streams.get(key);
    return pending.append(stream.getMessageId(), request.getMessage());
  }

  CompletableFuture<ByteString> streamCloseAsync(RaftClientRequest request) {
    final StreamRequestTypeProto stream = request.getType().getStream();
    Preconditions.assertTrue(stream.getClose());
    final Key key = new Key(request.getClientId(), stream.getStreamId());

    final PendingStream pending = streams.remove(key);
    if (pending == null) {
      return JavaUtils.completeExceptionally(new StreamException(name + ": " + key + " not found"));
    }
    return pending.getBytes(stream.getMessageId(), request.getMessage());
  }

  void clear() {
    streams.clear();
  }
}
