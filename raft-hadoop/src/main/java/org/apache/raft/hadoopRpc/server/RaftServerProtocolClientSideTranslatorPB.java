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

import com.google.protobuf.ServiceException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ipc.RPC;
import org.apache.raft.hadoopRpc.HadoopUtils;
import org.apache.raft.proto.RaftServerProtocolProtos.*;
import org.apache.raft.server.protocol.*;
import org.apache.raft.util.ProtoUtils;

import java.io.Closeable;
import java.io.IOException;

@InterfaceAudience.Private
public class RaftServerProtocolClientSideTranslatorPB
    implements RaftServerProtocol, Closeable {
  private final RaftServerProtocolPB rpcProxy;

  public RaftServerProtocolClientSideTranslatorPB(RaftServerProtocolPB rpcProxy) {
    this.rpcProxy = rpcProxy;
  }

  @Override
  public void close() throws IOException {
    RPC.stopProxy(rpcProxy);
  }

  @Override
  public RequestVoteReply requestVote(RequestVoteRequest request) throws IOException {
    final RequestVoteRequestProto p = HadoopUtils.toRequestVoteRequestProto(request);
    final RequestVoteReplyProto reply;
    try {
      reply = rpcProxy.requestVote(null, p);
    } catch (ServiceException se) {
      throw ProtoUtils.toIOException(se);
    }
    return HadoopUtils.toRequestVoteReply(reply);
  }

  @Override
  public AppendEntriesReply appendEntries(AppendEntriesRequest request) throws IOException {
    final AppendEntriesRequestProto p = HadoopUtils.toAppendEntriesRequestProto(request);
    final AppendEntriesReplyProto reply;
    try {
      reply = rpcProxy.appendEntries(null, p);
    } catch (ServiceException se) {
      throw ProtoUtils.toIOException(se);
    }
    return HadoopUtils.toAppendEntriesReply(reply);
  }

  @Override
  public InstallSnapshotReply installSnapshot(InstallSnapshotRequest request)
      throws IOException {
    final InstallSnapshotRequestProto p = HadoopUtils
        .toInstallSnapshotRequestProto(request);
    final InstallSnapshotReplyProto replyProto;
    try {
      replyProto = rpcProxy.installSnapshot(null, p);
    } catch (ServiceException se) {
      throw ProtoUtils.toIOException(se);
    }
    return HadoopUtils.toInstallSnapshotReply(replyProto);
  }
}
