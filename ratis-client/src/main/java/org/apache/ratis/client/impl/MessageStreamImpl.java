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

import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.client.api.MessageOutputStream;
import org.apache.ratis.client.api.MessageStreamApi;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftClientRequest.Type;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.SizeInBytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/** Send ordered asynchronous requests to a raft service. */
public final class MessageStreamImpl implements MessageStreamApi {
  public static final Logger LOG = LoggerFactory.getLogger(MessageStreamImpl.class);

  static MessageStreamImpl newInstance(RaftClientImpl client, RaftProperties properties) {
    return new MessageStreamImpl(client, properties);
  }

  class MessageOutputStreamImpl implements MessageOutputStream {
    private final long id;
    private final AtomicLong messageId = new AtomicLong();

    MessageOutputStreamImpl(long id) {
      this.id = id;
    }

    private Type getMessageStreamRequestType(boolean endOfRequest) {
      return RaftClientRequest.messageStreamRequestType(id, messageId.getAndIncrement(), endOfRequest);
    }

    @Override
    public CompletableFuture<RaftClientReply> sendAsync(Message message, boolean endOfRequest) {
      return client.async().send(getMessageStreamRequestType(endOfRequest), message, null);
    }

    @Override
    public CompletableFuture<RaftClientReply> closeAsync() {
      return client.async().send(getMessageStreamRequestType(true), null, null);
    }
  }

  private final RaftClientImpl client;
  private final SizeInBytes submessageSize;
  private final AtomicLong streamId = new AtomicLong();

  private MessageStreamImpl(RaftClientImpl client, RaftProperties properties) {
    this.client = Objects.requireNonNull(client, "client == null");
    this.submessageSize = RaftClientConfigKeys.MessageStream.submessageSize(properties);
  }

  @Override
  public MessageOutputStream stream() {
    return new MessageOutputStreamImpl(streamId.incrementAndGet());
  }

  @Override
  public CompletableFuture<RaftClientReply> streamAsync(Message message, SizeInBytes subSize) {
    final int n = subSize.getSizeInt();
    final MessageOutputStream out = stream();
    final ByteString bytes = message.getContent();
    for(int i = 0; i < bytes.size(); ) {
      final int j = Math.min(i + n, bytes.size());
      final ByteString sub = bytes.substring(i, j);
      out.sendAsync(Message.valueOf(sub));
      i = j;
    }
    return out.closeAsync();
  }

  @Override
  public CompletableFuture<RaftClientReply> streamAsync(Message message) {
    return streamAsync(message, submessageSize);
  }
}
