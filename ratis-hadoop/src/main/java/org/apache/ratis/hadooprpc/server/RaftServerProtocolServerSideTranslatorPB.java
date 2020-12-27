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
package org.apache.ratis.hadooprpc.server;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.ratis.proto.hadoop.HadoopCompatibilityProtos.ServerOps;
import org.apache.ratis.proto.hadoop.HadoopCompatibilityProtos.ServerReplyProto;
import org.apache.ratis.proto.hadoop.HadoopCompatibilityProtos.ServerRequestProto;
import org.apache.ratis.server.protocol.RaftServerProtocol;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotReplyProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotRequestProto;
import org.apache.ratis.proto.RaftProtos.RequestVoteReplyProto;
import org.apache.ratis.proto.RaftProtos.RequestVoteRequestProto;
import org.apache.ratis.proto.RaftProtos.StartLeaderElectionReplyProto;
import org.apache.ratis.proto.RaftProtos.StartLeaderElectionRequestProto;
import org.apache.ratis.thirdparty.com.google.protobuf.GeneratedMessageV3;

@InterfaceAudience.Private
public class RaftServerProtocolServerSideTranslatorPB
    implements RaftServerProtocolPB {
  private final RaftServerProtocol impl;

  public RaftServerProtocolServerSideTranslatorPB(RaftServerProtocol impl) {
    this.impl = impl;
  }

  @Override
  public ServerReplyProto sendServer(RpcController unused, ServerRequestProto requestProto) throws ServiceException {
    ServerOps type = requestProto.getType();
    ByteBuffer buffer = requestProto.getRequest().asReadOnlyByteBuffer();
    GeneratedMessageV3 respone = null;
    try {
      switch (type) {
        case requestVote:
          respone = requestVote(RequestVoteRequestProto.parseFrom(buffer));
          break;
        case startLeaderElection:
          respone = startLeaderElection(StartLeaderElectionRequestProto.parseFrom(buffer));
          break;
        case installSnapshot:
          respone = installSnapshot(InstallSnapshotRequestProto.parseFrom(buffer));
          break;
        case appendEntries:
          respone = appendEntries(AppendEntriesRequestProto.parseFrom(buffer));
          break;
        default:
          throw new IOException("Invalid Request Type:" + type);
      }
      return ServerReplyProto.newBuilder()
          .setType(type)
          .setResponse(com.google.protobuf.ByteString.copyFrom(respone.toByteArray()))
          .build();
    } catch(IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  public RequestVoteReplyProto requestVote(RequestVoteRequestProto request)
      throws IOException {
    return impl.requestVote(request);
  }

  public StartLeaderElectionReplyProto startLeaderElection(StartLeaderElectionRequestProto request) throws IOException {
    return impl.startLeaderElection(request);
  }

  public AppendEntriesReplyProto appendEntries(AppendEntriesRequestProto request)
      throws IOException {
    return impl.appendEntries(request);
  }

  public InstallSnapshotReplyProto installSnapshot(
      InstallSnapshotRequestProto request) throws IOException {
    return impl.installSnapshot(request);
  }
}
