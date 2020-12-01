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
package org.apache.ratis.client;

import org.apache.ratis.client.api.AsyncApi;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;

import java.util.concurrent.CompletableFuture;

/** An RPC interface which extends the user interface {@link AsyncApi}. */
public interface AsyncRpcApi extends AsyncApi {
  /**
   * Send the given RaftClientRequest asynchronously to the raft service.
   * The RaftClientRequest will wrapped as Message in a new RaftClientRequest
   * and leader will be decode it from the Message
   * @param request The RaftClientRequest.
   * @return a future of the reply.
   */
  CompletableFuture<RaftClientReply> sendForward(RaftClientRequest request);
}
