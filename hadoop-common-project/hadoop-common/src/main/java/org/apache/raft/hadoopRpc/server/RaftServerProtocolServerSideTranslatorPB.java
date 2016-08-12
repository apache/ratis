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
package org.apache.raft.hadoopRpc.server;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.raft.proto.RaftServerProtocolProtos.*;
import org.apache.raft.server.protocol.AppendEntriesReply;
import org.apache.raft.server.protocol.InstallSnapshotReply;
import org.apache.raft.server.protocol.RaftServerProtocol;
import org.apache.raft.server.protocol.RequestVoteReply;
import org.apache.raft.server.protocol.pb.ServerProtoUtils;

import java.io.IOException;

@InterfaceAudience.Private
public class RaftServerProtocolServerSideTranslatorPB
    implements RaftServerProtocolPB {
  private final RaftServerProtocol impl;

  public RaftServerProtocolServerSideTranslatorPB(RaftServerProtocol impl) {
    this.impl = impl;
  }

  @Override
  public RequestVoteReplyProto requestVote(
      RpcController unused, RequestVoteRequestProto request)
      throws ServiceException {
    final RequestVoteReply reply;
    try {
      reply = impl.requestVote(ServerProtoUtils.toRequestVoteRequest(request));
    } catch(IOException ioe) {
      throw new ServiceException(ioe);
    }
    return ServerProtoUtils.toRequestVoteReplyProto(request, reply);
  }

  @Override
  public AppendEntriesReplyProto appendEntries(
      RpcController unused, AppendEntriesRequestProto request)
      throws ServiceException {
    final AppendEntriesReply reply;
    try {
      reply = impl.appendEntries(ServerProtoUtils.toAppendEntriesRequest(request));
    } catch(IOException ioe) {
      throw new ServiceException(ioe);
    }
    return ServerProtoUtils.toAppendEntriesReplyProto(request, reply);
  }

  @Override
  public InstallSnapshotReplyProto installSnapshot(RpcController controller,
      InstallSnapshotRequestProto request) throws ServiceException {
    final InstallSnapshotReply reply;
    try {
      reply = impl.installSnapshot(
          ServerProtoUtils.toInstallSnapshotRequest(request));
      return ServerProtoUtils.toInstallSnapshotReplyProto(request, reply);
    } catch(IOException ioe) {
      throw new ServiceException(ioe);
    }
  }
}
