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
package org.apache.hadoop.raft.protocol.pb;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.raft.proto.RaftClientProtocolProtos.*;
import org.apache.hadoop.raft.protocol.RaftClientProtocol;
import org.apache.hadoop.raft.protocol.RaftClientReply;
import org.apache.hadoop.raft.protocol.RaftClientRequest;
import org.apache.hadoop.raft.protocol.SetConfigurationRequest;
import org.apache.hadoop.raft.server.protocol.RaftServerProtocol;
import org.apache.hadoop.util.ProtoUtil;

import java.io.IOException;

@InterfaceAudience.Private
public class RaftClientProtocolServerSideTranslatorPB
    implements RaftClientProtocolPB {
  private final RaftClientProtocol impl;

  public RaftClientProtocolServerSideTranslatorPB(RaftClientProtocol impl) {
    this.impl = impl;
  }

  @Override
  public RaftClientReplyProto submitClientRequest(
      RpcController unused, RaftClientRequestProto proto)
      throws ServiceException {
    final RaftClientRequest request = ProtoUtils.toRaftClientRequest(proto);
    try {
      impl.submitClientRequest(request);
    } catch(IOException ioe) {
      throw new ServiceException(ioe);
    }
    final RaftClientReply reply = new RaftClientReply(request, true);
    return ProtoUtils.toRaftClientReplyProto(proto, reply);
  }

  @Override
  public SetConfigurationReplyProto setConfiguration(
      RpcController unused, SetConfigurationRequestProto proto)
      throws ServiceException {
    final SetConfigurationRequest request;
    try {
      request = ProtoUtils.toSetConfigurationRequest(proto);
      impl.setConfiguration(request);
    } catch(IOException ioe) {
      throw new ServiceException(ioe);
    }
    final RaftClientReply reply = new RaftClientReply(request, true);
    return ProtoUtils.toSetConfigurationReplyProto(proto, reply);
  }
}
