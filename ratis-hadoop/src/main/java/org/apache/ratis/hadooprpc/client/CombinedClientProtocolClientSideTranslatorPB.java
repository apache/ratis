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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.ratis.client.impl.ClientProtoUtils;
import org.apache.ratis.hadooprpc.Proxy;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.ReinitializeRequest;
import org.apache.ratis.protocol.SetConfigurationRequest;
import org.apache.ratis.shaded.com.google.common.base.Function;
import org.apache.ratis.shaded.com.google.protobuf.ServiceException;
import org.apache.ratis.shaded.proto.RaftProtos.RaftClientReplyProto;
import org.apache.ratis.util.CheckedFunction;
import org.apache.ratis.util.ProtoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


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
    return handleRequest(request, ClientProtoUtils::toRaftClientRequestProto,
        p -> getProtocol().submitClientRequest(null, p));
  }

  @Override
  public RaftClientReply setConfiguration(SetConfigurationRequest request)
      throws IOException {
    return handleRequest(request, ClientProtoUtils::toSetConfigurationRequestProto,
        p -> getProtocol().setConfiguration(null, p));
  }

  @Override
  public RaftClientReply reinitialize(ReinitializeRequest request) throws IOException {
    return handleRequest(request, ClientProtoUtils::toReinitializeRequestProto,
      p -> getProtocol().reinitialize(null, p));
  }

  static <REQUEST extends RaftClientRequest, PROTO> RaftClientReply handleRequest(
      REQUEST request, Function<REQUEST, PROTO> toProto,
      CheckedFunction<PROTO, RaftClientReplyProto, ServiceException> handler)
      throws IOException {
    final PROTO proto = toProto.apply(request);
    try {
      final RaftClientReplyProto reply = handler.apply(proto);
      return ClientProtoUtils.toRaftClientReply(reply);
    } catch (ServiceException se) {
      LOG.trace("Failed to handle " + request, se);
      throw ProtoUtils.toIOException(se);
    }
  }
}
