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
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.raft.proto.RaftServerProtocolProtos.AppendEntriesReplyProto;
import org.apache.hadoop.raft.proto.RaftServerProtocolProtos.AppendEntriesRequestProto;
import org.apache.hadoop.raft.proto.RaftServerProtocolProtos.RequestVoteReplyProto;
import org.apache.hadoop.raft.proto.RaftServerProtocolProtos.RequestVoteRequestProto;
import org.apache.hadoop.raft.protocol.pb.ProtoUtils;
import org.apache.hadoop.raft.server.protocol.*;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

@InterfaceAudience.Private
public class RaftServerProtocolClientSideTranslatorPB
    implements RaftServerProtocol, Closeable {
  private final RaftServerProtocolPB rpcProxy;

  public RaftServerProtocolClientSideTranslatorPB(RaftServerProtocolPB rpcProxy) {
    this.rpcProxy = rpcProxy;
  }

  public RaftServerProtocolClientSideTranslatorPB(
      InetSocketAddress address, Configuration conf) throws IOException {
    this(newRaftServerProtocolPB(address, conf));
  }

  private static RaftServerProtocolPB newRaftServerProtocolPB(
      InetSocketAddress address, Configuration conf) throws IOException {
    final Class<RaftServerProtocolPB> clazz = RaftServerProtocolPB.class;
    RPC.setProtocolEngine(conf, clazz, ProtobufRpcEngine.class);
    final UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    return RPC.getProxy(clazz, RPC.getProtocolVersion(clazz), address, ugi,
        conf, NetUtils.getSocketFactory(conf, clazz));
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
}
