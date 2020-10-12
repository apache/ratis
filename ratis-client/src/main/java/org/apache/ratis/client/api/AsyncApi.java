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

import java.util.concurrent.CompletableFuture;
import org.apache.ratis.proto.RaftProtos.ReplicationLevel;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeerId;

/**
 * APIs to support asynchronous operations such as send message, send (stale)read message and watch request.
 */
public interface AsyncApi {
  /**
   * Async call to send the given message to the raft service.
   * The message may change the state of the service.
   * For readonly messages, use {@link #sendReadOnlyAsync(Message)} instead.
   *
   * @param message The request message.
   * @return a future of the reply.
   */
  CompletableFuture<RaftClientReply> sendAsync(Message message);

  /** Async call to send the given readonly message to the raft service. */
  CompletableFuture<RaftClientReply> sendReadOnlyAsync(Message message);

  /** Async call to send the given stale-read message to the given server (not the raft service). */
  CompletableFuture<RaftClientReply> sendStaleReadAsync(Message message, long minIndex, RaftPeerId server);

  /** Async call to watch the given index to satisfy the given replication level. */
  CompletableFuture<RaftClientReply> sendWatchAsync(long index, ReplicationLevel replication);
}
