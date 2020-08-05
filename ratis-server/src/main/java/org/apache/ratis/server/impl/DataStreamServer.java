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

import org.apache.ratis.datastream.objects.DataStreamReply;
import org.apache.ratis.datastream.objects.DataStreamRequest;

import java.util.concurrent.CompletableFuture;

/**
 * A server interface handling incoming streams
 * Relays those streams to other servers after persisting
 * It will have an associated Netty client, server for streaming and listening.
 */
public interface DataStreamServer {
  /**
   * Invoked from the server to persist data and add to relay queue.
   */
  boolean handleRequest(DataStreamRequest request);

  /**
   * Poll the queue and trigger streaming for messages in relay queue.
   */
  CompletableFuture<DataStreamReply> streamAsync(DataStreamRequest request);

  /**
   * receive a reply from the client and set the necessary future.
   * Invoked by the Netty Client associated with the object.
   */
  CompletableFuture<DataStreamReply> setReply(DataStreamReply reply);
}
