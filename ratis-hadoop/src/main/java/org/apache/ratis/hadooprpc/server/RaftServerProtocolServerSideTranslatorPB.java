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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.ratis.server.protocol.RaftServerProtocol;
import org.apache.ratis.shaded.com.google.protobuf.RpcController;
import org.apache.ratis.shaded.com.google.protobuf.ServiceException;
import org.apache.ratis.shaded.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.ratis.shaded.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.shaded.proto.RaftProtos.InstallSnapshotReplyProto;
import org.apache.ratis.shaded.proto.RaftProtos.InstallSnapshotRequestProto;
import org.apache.ratis.shaded.proto.RaftProtos.RequestVoteReplyProto;
import org.apache.ratis.shaded.proto.RaftProtos.RequestVoteRequestProto;

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
    try {
      return impl.requestVote(request);
    } catch(IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  @Override
  public AppendEntriesReplyProto appendEntries(
      RpcController unused, AppendEntriesRequestProto request)
      throws ServiceException {
    try {
      return impl.appendEntries(request);
    } catch(IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  @Override
  public InstallSnapshotReplyProto installSnapshot(RpcController controller,
      InstallSnapshotRequestProto request) throws ServiceException {
    try {
      return impl.installSnapshot(request);
    } catch(IOException ioe) {
      throw new ServiceException(ioe);
    }
  }
}
