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
package org.apache.ratis.client;

import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/** The client side rpc of a raft service. */
public interface RaftClientRpc extends Closeable {
  /** Async call to send a request. */
  default CompletableFuture<RaftClientReply> sendRequestAsync(RaftClientRequest request) {
    throw new UnsupportedOperationException(getClass() + " does not support this method.");
  }

  /** Send a request. */
  RaftClientReply sendRequest(RaftClientRequest request) throws IOException;

  /** Add the information of the given raft servers */
  void addServers(Iterable<RaftPeer> servers);

  /** Handle the given exception.  For example, try reconnecting. */
  void handleException(RaftPeerId serverId, Exception e, boolean shouldClose);
}
