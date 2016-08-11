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
package org.apache.hadoop.raft.server.protocol.pb;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.raft.hadoopRpc.HadoopUtils;
import org.apache.hadoop.raft.proto.RaftServerProtocolProtos.AppendEntriesReplyProto;
import org.apache.hadoop.raft.proto.RaftServerProtocolProtos.AppendEntriesRequestProto;
import org.apache.hadoop.raft.proto.RaftServerProtocolProtos.InstallSnapshotReplyProto;
import org.apache.hadoop.raft.proto.RaftServerProtocolProtos.InstallSnapshotRequestProto;
import org.apache.hadoop.raft.proto.RaftServerProtocolProtos.RequestVoteReplyProto;
import org.apache.hadoop.raft.proto.RaftServerProtocolProtos.RequestVoteRequestProto;
import org.apache.hadoop.raft.protocol.pb.ProtoUtils;
import org.apache.hadoop.raft.server.protocol.*;

import java.io.Closeable;
import java.io.IOException;

@InterfaceAudience.Private
public class RaftServerProtocolClientSideTranslatorPB
    implements RaftServerProtocol, Closeable {
  private final RaftServerProtocolPB rpcProxy;

  public RaftServerProtocolClientSideTranslatorPB(RaftServerProtocolPB rpcProxy) {
    this.rpcProxy = rpcProxy;
  }

  public RaftServerProtocolClientSideTranslatorPB(
      String address, Configuration conf) throws IOException {
    this(HadoopUtils.getProxy(RaftServerProtocolPB.class, address, conf));
  }

  @Override
  public void close() throws IOException {
    RPC.stopProxy(rpcProxy);
  }

  @Override
  public RequestVoteReply requestVote(RequestVoteRequest request) throws IOException {
    final RequestVoteRequestProto p = ServerProtoUtils.toRequestVoteRequestProto(request);
    final RequestVoteReplyProto reply;
    try {
      reply = rpcProxy.requestVote(null, p);
    } catch (ServiceException se) {
      throw ProtoUtils.toIOException(se);
    }
    return ServerProtoUtils.toRequestVoteReply(reply);
  }

  @Override
  public AppendEntriesReply appendEntries(AppendEntriesRequest request) throws IOException {
    final AppendEntriesRequestProto p = ServerProtoUtils.toAppendEntriesRequestProto(request);
    final AppendEntriesReplyProto reply;
    try {
      reply = rpcProxy.appendEntries(null, p);
    } catch (ServiceException se) {
      throw ProtoUtils.toIOException(se);
    }
    return ServerProtoUtils.toAppendEntriesReply(reply);
  }

  @Override
  public InstallSnapshotReply installSnapshot(InstallSnapshotRequest request)
      throws IOException {
    final InstallSnapshotRequestProto p = ServerProtoUtils
        .toInstallSnapshotRequestProto(request);
    final InstallSnapshotReplyProto replyProto;
    try {
      replyProto = rpcProxy.installSnapshot(null, p);
    } catch (ServiceException se) {
      throw ProtoUtils.toIOException(se);
    }
    return ServerProtoUtils.toInstallSnapshotReply(replyProto);
  }
}
