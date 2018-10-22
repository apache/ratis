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
package org.apache.ratis.hadooprpc.client;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.ratis.client.impl.ClientProtoUtils;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.thirdparty.com.google.protobuf.RpcController;
import org.apache.ratis.thirdparty.com.google.protobuf.ServiceException;
import org.apache.ratis.proto.RaftProtos.RaftClientReplyProto;
import org.apache.ratis.proto.RaftProtos.RaftClientRequestProto;
import org.apache.ratis.proto.RaftProtos.SetConfigurationRequestProto;
import org.apache.ratis.proto.RaftProtos.GroupManagementRequestProto;
import org.apache.ratis.proto.RaftProtos.GroupListRequestProto;
import org.apache.ratis.proto.RaftProtos.GroupListReplyProto;
import org.apache.ratis.proto.RaftProtos.GroupInfoRequestProto;
import org.apache.ratis.proto.RaftProtos.GroupInfoReplyProto;


@InterfaceAudience.Private
public class CombinedClientProtocolServerSideTranslatorPB
    implements CombinedClientProtocolPB {
  private final RaftServer impl;

  public CombinedClientProtocolServerSideTranslatorPB(RaftServer impl) {
    this.impl = impl;
  }

  @Override
  public RaftClientReplyProto submitClientRequest(
      RpcController unused, RaftClientRequestProto proto)
      throws ServiceException {
    final RaftClientRequest request = ClientProtoUtils.toRaftClientRequest(proto);
    try {
      final RaftClientReply reply = impl.submitClientRequest(request);
      return ClientProtoUtils.toRaftClientReplyProto(reply);
    } catch(IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  @Override
  public RaftClientReplyProto setConfiguration(
      RpcController unused, SetConfigurationRequestProto proto)
      throws ServiceException {
    final SetConfigurationRequest request;
    try {
      request = ClientProtoUtils.toSetConfigurationRequest(proto);
      final RaftClientReply reply = impl.setConfiguration(request);
      return ClientProtoUtils.toRaftClientReplyProto(reply);
    } catch(IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  @Override
  public RaftClientReplyProto groupManagement(RpcController controller, GroupManagementRequestProto proto)
      throws ServiceException {
    final GroupManagementRequest request;
    try {
      request = ClientProtoUtils.toGroupManagementRequest(proto);
      final RaftClientReply reply = impl.groupManagement(request);
      return ClientProtoUtils.toRaftClientReplyProto(reply);
    } catch(IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  @Override
  public GroupListReplyProto groupList(
      RpcController controller, GroupListRequestProto proto)
    throws ServiceException {
    final GroupListRequest request;
    try {
      request = ClientProtoUtils.toGroupListRequest(proto);
      final GroupListReply reply = impl.getGroupList(request);
      return ClientProtoUtils.toGroupListReplyProto(reply);
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  @Override
  public GroupInfoReplyProto groupInfo(RpcController controller, GroupInfoRequestProto proto) throws ServiceException {
    final GroupInfoRequest request;
    try {
      request = ClientProtoUtils.toGroupInfoRequest(proto);
      final GroupInfoReply reply = impl.getGroupInfo(request);
      return ClientProtoUtils.toGroupInfoReplyProto(reply);
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }
}
