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
package org.apache.ratis.client.api;

import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.util.SizeInBytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * Stream (large) {@link Message}(s) asynchronously to the leader.
 * Once a stream has been closed or has been signaled an end-of-request,
 * the leader creates a raft log entry for the request
 * and then replicates the log entry to all the followers.
 *
 * Note that this API is similar to {@link AsyncApi#send(Message)}
 * except that {@link MessageStreamApi} divides a (large) message into multiple (small) sub-messages in the stream
 * but {@link AsyncApi#send(Message)} sends the entire message in a single RPC.
 * For sending large messages,
 * {@link MessageStreamApi} is more efficient than {@link AsyncApi#send(Message)}}.
 *
 * Note also that this API is different from {@link DataStreamApi} in the sense that
 * this API streams messages only to the leader
 * but {@link DataStreamApi} streams data to all the servers in the {@link org.apache.ratis.protocol.RaftGroup}.
 */
public interface MessageStreamApi {
  Logger LOG = LoggerFactory.getLogger(MessageStreamApi.class);

  /** Create a stream to send a large message. */
  MessageOutputStream stream();

  /** Send the given (large) message using a stream with the submessage size. */
  CompletableFuture<RaftClientReply> streamAsync(Message message, SizeInBytes submessageSize);

  /** Send the given message using a stream with submessage size specified in conf. */
  CompletableFuture<RaftClientReply> streamAsync(Message message);
}
