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
package org.apache.hadoop.raft.server;

import org.apache.hadoop.raft.protocol.RaftClientReply;
import org.apache.hadoop.raft.protocol.RaftClientRequest;
import org.apache.hadoop.raft.protocol.SetConfigurationRequest;
import org.apache.hadoop.raft.server.protocol.AppendEntriesRequest;
import org.apache.hadoop.raft.server.protocol.RaftServerReply;
import org.apache.hadoop.raft.server.protocol.RaftServerRequest;
import org.apache.hadoop.raft.server.protocol.RequestVoteRequest;

import java.io.IOException;
import java.net.InetSocketAddress;

public interface RaftServerRpc {
  void start();

  void interruptAndJoin() throws InterruptedException;

  void shutdown();

  InetSocketAddress getInetSocketAddress();

  RaftServerReply sendServerRequest(RaftServerRequest request) throws IOException;

  /** Save call info so that it can reply asynchronously. */
  void saveCallInfo(PendingRequest pending) throws IOException;

  void sendClientReply(RaftClientRequest request, RaftClientReply reply, IOException ioe)
      throws IOException;
}
