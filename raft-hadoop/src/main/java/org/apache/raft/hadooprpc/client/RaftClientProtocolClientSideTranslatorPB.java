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
package org.apache.raft.hadooprpc.client;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.raft.client.impl.ClientProtoUtils;
import org.apache.raft.hadooprpc.Proxy;
import org.apache.raft.protocol.RaftClientProtocol;
import org.apache.raft.protocol.RaftClientReply;
import org.apache.raft.protocol.RaftClientRequest;
import org.apache.raft.protocol.SetConfigurationRequest;
import org.apache.raft.shaded.com.google.protobuf.ServiceException;
import org.apache.raft.shaded.proto.RaftProtos.RaftClientReplyProto;
import org.apache.raft.shaded.proto.RaftProtos.RaftClientRequestProto;
import org.apache.raft.shaded.proto.RaftProtos.SetConfigurationRequestProto;
import org.apache.raft.util.ProtoUtils;

import java.io.IOException;

@InterfaceAudience.Private
public class RaftClientProtocolClientSideTranslatorPB
    extends Proxy<RaftClientProtocolPB>
    implements RaftClientProtocol {

  public RaftClientProtocolClientSideTranslatorPB(
      String addressStr, Configuration conf) throws IOException {
    super(RaftClientProtocolPB.class, addressStr, conf);
  }

  @Override
  public RaftClientReply submitClientRequest(RaftClientRequest request)
      throws IOException {
    final RaftClientRequestProto p = ClientProtoUtils.toRaftClientRequestProto(request);
    try {
      final RaftClientReplyProto reply = getProtocol().submitClientRequest(null, p);
      return ClientProtoUtils.toRaftClientReply(reply);
    } catch (ServiceException se) {
      throw ProtoUtils.toIOException(se);
    }
  }

  @Override
  public RaftClientReply setConfiguration(SetConfigurationRequest request)
      throws IOException {
    final SetConfigurationRequestProto p
        = ClientProtoUtils.toSetConfigurationRequestProto(request);
    try {
      final RaftClientReplyProto reply = getProtocol().setConfiguration(null, p);
      return ClientProtoUtils.toRaftClientReply(reply);
    } catch (ServiceException se) {
      throw ProtoUtils.toIOException(se);
    }
  }
}
