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
package org.apache.ratis.hadooprpc.client;

import com.google.protobuf.ByteString;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.ratis.client.impl.ClientProtoUtils;
import org.apache.ratis.hadooprpc.Proxy;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.proto.hadoop.HadoopCompatibilityProtos.ClientRequestProto;
import org.apache.ratis.proto.hadoop.HadoopCompatibilityProtos.ClientOps;
import org.apache.ratis.protocol.GroupInfoReply;
import org.apache.ratis.protocol.GroupInfoRequest;
import org.apache.ratis.protocol.GroupListReply;
import org.apache.ratis.protocol.GroupListRequest;
import org.apache.ratis.protocol.GroupManagementRequest;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.SetConfigurationRequest;
import org.apache.ratis.protocol.TransferLeadershipRequest;
import org.apache.ratis.thirdparty.com.google.protobuf
    .GeneratedMessageV3;
import org.apache.ratis.thirdparty.com.google.protobuf
    .InvalidProtocolBufferException;
import org.apache.ratis.util.function.CheckedFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.function.Function;


@InterfaceAudience.Private
public class CombinedClientProtocolClientSideTranslatorPB
    extends Proxy<CombinedClientProtocolPB>
    implements CombinedClientProtocol {
  private static final Logger LOG = LoggerFactory.getLogger(CombinedClientProtocolClientSideTranslatorPB.class);

  public CombinedClientProtocolClientSideTranslatorPB(
      String addressStr, Configuration conf) throws IOException {
    super(CombinedClientProtocolPB.class, addressStr, conf);
  }

  @Override
  public RaftClientReply submitClientRequest(RaftClientRequest request)
      throws IOException {
    return handleRequest(request,
        ClientProtoUtils::toRaftClientRequestProto,
        ClientProtoUtils::toRaftClientReply,
        ClientOps.submitClientRequest,
        RaftProtos.RaftClientReplyProto::parseFrom);
  }

  @Override
  public RaftClientReply setConfiguration(SetConfigurationRequest request)
      throws IOException {
    return handleRequest(request,
        ClientProtoUtils::toSetConfigurationRequestProto,
        ClientProtoUtils::toRaftClientReply,
        ClientOps.setConfiguration,
        RaftProtos.RaftClientReplyProto::parseFrom);
  }

  @Override
  public RaftClientReply transferLeadership(TransferLeadershipRequest request)
      throws IOException {
    return handleRequest(request,
        ClientProtoUtils::toTransferLeadershipRequestProto,
        ClientProtoUtils::toRaftClientReply,
        ClientOps.transferLeadership,
        RaftProtos.RaftClientReplyProto::parseFrom);
  }

  @Override
  public RaftClientReply groupManagement(GroupManagementRequest request) throws IOException {
    return handleRequest(request,
        ClientProtoUtils::toGroupManagementRequestProto,
        ClientProtoUtils::toRaftClientReply,
        ClientOps.groupManagement,
        RaftProtos.RaftClientReplyProto::parseFrom);
  }

  @Override
  public GroupListReply getGroupList(GroupListRequest request) throws IOException {
    return handleRequest(request,
        ClientProtoUtils::toGroupListRequestProto,
        ClientProtoUtils::toGroupListReply,
        ClientOps.groupList,
        RaftProtos.GroupListReplyProto::parseFrom);
  }

  @Override
  public GroupInfoReply getGroupInfo(GroupInfoRequest request) throws IOException {
    return handleRequest(request,
        ClientProtoUtils::toGroupInfoRequestProto,
        ClientProtoUtils::toGroupInfoReply,
        ClientOps.groupInfo,
        RaftProtos.GroupInfoReplyProto::parseFrom);
  }

  <REQUEST extends RaftClientRequest,
      REPLY extends RaftClientReply,
      PROTO_REQ extends GeneratedMessageV3,
      PROTO_REP extends GeneratedMessageV3> REPLY handleRequest(
      REQUEST request,
      Function<REQUEST, PROTO_REQ> reqToProto,
      Function<PROTO_REP, REPLY> repToProto,
      ClientOps type,
      CheckedFunction<byte[], PROTO_REP, InvalidProtocolBufferException> byteToProto)
      throws IOException {
    final PROTO_REQ proto = reqToProto.apply(request);
    try {
      ClientRequestProto req = ClientRequestProto.newBuilder()
          .setType(type)
          .setRequest(ByteString.copyFrom(proto.toByteArray()))
          .build();
      byte[] reply = getProtocol().sendClient(null, req)
          .getResponse().toByteArray();

      PROTO_REP replyProto = byteToProto.apply(reply);
      return repToProto.apply(replyProto);
    } catch (ServiceException se) {
      LOG.trace("Failed to handle " + request, se);
      throw new IOException(se);
    }
  }
}
