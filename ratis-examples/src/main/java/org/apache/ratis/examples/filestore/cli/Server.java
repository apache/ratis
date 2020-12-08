/*
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
package org.apache.ratis.examples.filestore.cli;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.conf.ConfUtils;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.datastream.SupportedDataStreamType;
import org.apache.ratis.examples.common.SubCommandBase;
import org.apache.ratis.examples.filestore.FileStoreCommon;
import org.apache.ratis.examples.filestore.FileStoreStateMachine;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.metrics.JVMMetrics;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.RaftServerProxy;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.NetUtils;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Class to start a ratis arithmetic example server.
 */
@Parameters(commandDescription = "Start an filestore server")
public class Server extends SubCommandBase {

  @Parameter(names = {"--id", "-i"}, description = "Raft id of this server", required = true)
  private String id;

  @Parameter(names = {"--storage", "-s"}, description = "Storage dir", required = true)
  private File storageDir;

  @Override
  public void run() throws Exception {
    JVMMetrics.initJvmMetrics(TimeDuration.valueOf(10, TimeUnit.SECONDS));

    RaftPeerId peerId = RaftPeerId.valueOf(id);
    RaftProperties properties = new RaftProperties();

    // Avoid leader change affect the performance
    RaftServerConfigKeys.Rpc.setTimeoutMin(properties, TimeDuration.valueOf(2, TimeUnit.SECONDS));
    RaftServerConfigKeys.Rpc.setTimeoutMax(properties, TimeDuration.valueOf(3, TimeUnit.SECONDS));

    final int port = NetUtils.createSocketAddr(getPeer(peerId).getAddress()).getPort();
    GrpcConfigKeys.Server.setPort(properties, port);
    final int dataStreamport = NetUtils.createSocketAddr(getPeer(peerId).getDataStreamAddress()).getPort();
    NettyConfigKeys.DataStream.setPort(properties, dataStreamport);
    RaftConfigKeys.DataStream.setType(properties, SupportedDataStreamType.NETTY);
    properties.setInt(GrpcConfigKeys.OutputStream.RETRY_TIMES_KEY, Integer.MAX_VALUE);
    RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storageDir));
    RaftServerConfigKeys.Write.setElementLimit(properties, 40960);
    RaftServerConfigKeys.Write.setByteLimit(properties, SizeInBytes.valueOf("1000MB"));
    ConfUtils.setFile(properties::setFile, FileStoreCommon.STATEMACHINE_DIR_KEY,
        storageDir);
    StateMachine stateMachine = new FileStoreStateMachine(properties);

    final RaftGroup raftGroup = RaftGroup.valueOf(RaftGroupId.valueOf(ByteString.copyFromUtf8(getRaftGroupId())),
            getPeers());
    RaftServer raftServer = RaftServer.newBuilder()
        .setServerId(RaftPeerId.valueOf(id))
        .setStateMachine(stateMachine).setProperties(properties)
        .setGroup(raftGroup)
        .build();

    raftServer.start();

    if (isPrimary(id)) {
      ((RaftServerProxy) raftServer).getDataStreamServerRpc()
          .addRaftPeers(getOtherRaftPeers(Arrays.asList(getPeers())));
    }

    for (; raftServer.getLifeCycleState() != LifeCycle.State.CLOSED; ) {
      TimeUnit.SECONDS.sleep(1);
    }
  }

  private Collection<RaftPeer> getOtherRaftPeers(Collection<RaftPeer> peers) {
    return peers.stream().filter(r -> !r.getId().toString().equals(id)).collect(Collectors.toList());
  }
}
