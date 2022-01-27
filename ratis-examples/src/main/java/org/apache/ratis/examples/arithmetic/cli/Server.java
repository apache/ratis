/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ratis.examples.arithmetic.cli;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.examples.arithmetic.ArithmeticStateMachine;
import org.apache.ratis.examples.common.SubCommandBase;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.NetUtils;

import java.io.File;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Class to start a ratis arithmetic example server.
 */
@Parameters(commandDescription = "Start an arithmetic server")
public class Server extends SubCommandBase {

  @Parameter(names = {"--id",
      "-i"}, description = "Raft id of this server", required = true)
  private String id;

  @Parameter(names = {"--storage",
      "-s"}, description = "Storage dir", required = true)
  private File storageDir;


  @Override
  public void run() throws Exception {
    RaftPeerId peerId = RaftPeerId.valueOf(id);
    RaftProperties properties = new RaftProperties();

    final int port = NetUtils.createSocketAddr(getPeer(peerId).getAddress()).getPort();
    GrpcConfigKeys.Server.setPort(properties, port);

    Optional.ofNullable(getPeer(peerId).getClientAddress()).ifPresent(address ->
        GrpcConfigKeys.Client.setPort(properties, NetUtils.createSocketAddr(address).getPort()));
    Optional.ofNullable(getPeer(peerId).getAdminAddress()).ifPresent(address ->
        GrpcConfigKeys.Admin.setPort(properties, NetUtils.createSocketAddr(address).getPort()));

    RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storageDir));
    StateMachine stateMachine = new ArithmeticStateMachine();

    final RaftGroup raftGroup = RaftGroup.valueOf(RaftGroupId.valueOf(ByteString.copyFromUtf8(getRaftGroupId())),
            getPeers());
    RaftServer raftServer = RaftServer.newBuilder()
        .setServerId(RaftPeerId.valueOf(id))
        .setStateMachine(stateMachine).setProperties(properties)
        .setGroup(raftGroup)
        .build();
    raftServer.start();

    for(; raftServer.getLifeCycleState() != LifeCycle.State.CLOSED;) {
      TimeUnit.SECONDS.sleep(1);
    }
  }

}
