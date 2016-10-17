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
package org.apache.raft.server;

import org.apache.raft.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.raft.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.raft.proto.RaftProtos.InstallSnapshotReplyProto;
import org.apache.raft.proto.RaftProtos.InstallSnapshotRequestProto;
import org.apache.raft.proto.RaftProtos.RequestVoteReplyProto;
import org.apache.raft.proto.RaftProtos.RequestVoteRequestProto;
import org.apache.raft.protocol.RaftPeer;

import java.io.IOException;
import java.net.InetSocketAddress;

public interface RaftServerRpc {
  void start();

  void shutdown();

  InetSocketAddress getInetSocketAddress();

  AppendEntriesReplyProto sendAppendEntries(
      AppendEntriesRequestProto request) throws IOException;

  InstallSnapshotReplyProto sendInstallSnapshot(
      InstallSnapshotRequestProto request) throws IOException;

  RequestVoteReplyProto sendRequestVote(RequestVoteRequestProto request)
      throws IOException;

  /** add rpc information of the given peers */
  void addPeerProxies(Iterable<RaftPeer> peers);
}
