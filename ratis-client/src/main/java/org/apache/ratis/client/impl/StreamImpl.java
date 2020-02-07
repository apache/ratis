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

import org.apache.ratis.client.api.MessageOutputStream;
import org.apache.ratis.client.api.StreamApi;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/** Send ordered asynchronous requests to a raft service. */
public final class StreamImpl implements StreamApi {
  public static final Logger LOG = LoggerFactory.getLogger(StreamImpl.class);

  static StreamImpl newInstance(RaftClientImpl client, RaftProperties properties) {
    return new StreamImpl(client, properties);
  }

  class MessageOutputStreamImpl implements MessageOutputStream {
    private final long id;
    private final AtomicLong messageId = new AtomicLong();

    MessageOutputStreamImpl(long id) {
      this.id = id;
    }

    @Override
    public CompletableFuture<RaftClientReply> sendAsync(Message message) {
      return client.streamAsync(id, messageId.getAndIncrement(), message);
    }

    @Override
    public CompletableFuture<RaftClientReply> closeAsync() {
      return client.streamCloseAsync(id, messageId.getAndIncrement());
    }
  }

  private final RaftClientImpl client;
  private final AtomicLong streamId = new AtomicLong();

  private StreamImpl(RaftClientImpl client, RaftProperties properties) {
    this.client = Objects.requireNonNull(client, "client == null");
  }

  @Override
  public MessageOutputStream stream() {
    return new MessageOutputStreamImpl(streamId.incrementAndGet());
  }
}
