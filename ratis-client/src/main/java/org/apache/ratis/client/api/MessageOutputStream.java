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

import org.apache.ratis.io.CloseAsync;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;

import java.util.concurrent.CompletableFuture;

/** Stream {@link Message}(s) asynchronously. */
public interface MessageOutputStream extends CloseAsync<RaftClientReply> {
  /**
   * Send asynchronously the given message to this stream.
   *
   * If end-of-request is true, this message is the last message of the request.
   * All the messages accumulated are considered as a single request.
   *
   * @param message the message to be sent.
   * @param endOfRequest Is this an end-of-request?
   * @return a future of the reply.
   */
  CompletableFuture<RaftClientReply> sendAsync(Message message, boolean endOfRequest);

  /** The same as sendAsync(message, false). */
  default CompletableFuture<RaftClientReply> sendAsync(Message message) {
    return sendAsync(message, false);
  }
}
