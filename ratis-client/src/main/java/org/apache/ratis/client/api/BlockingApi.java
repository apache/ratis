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

import java.io.IOException;
import org.apache.ratis.proto.RaftProtos.ReplicationLevel;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeerId;

/**
 * APIs to support blocking operations such as send message, send (stale)read message and watch request.
 */
public interface BlockingApi {
  /**
   * Send the given message to the raft service.
   * The message may change the state of the service.
   * For readonly messages, use {@link #sendReadOnly(Message)} instead.
   *
   * @param message The request message.
   * @return the reply.
   */
  RaftClientReply send(Message message) throws IOException;

  /** Send the given readonly message to the raft service. */
  RaftClientReply sendReadOnly(Message message) throws IOException;

  /** Send the given stale-read message to the given server (not the raft service). */
  RaftClientReply sendStaleRead(Message message, long minIndex, RaftPeerId server) throws IOException;

  /** Watch the given index to satisfy the given replication level. */
  RaftClientReply watch(long index, ReplicationLevel replication) throws IOException;
}
