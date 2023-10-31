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
package org.apache.ratis.examples.membership.server;

import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.examples.counter.server.CounterStateMachine;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.thirdparty.com.google.common.base.MoreObjects;
import org.apache.ratis.util.FileUtils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Collections;

/**
 * A simple raft server using {@link CounterStateMachine}.
 */
public class CServer implements Closeable {
  public static final RaftGroupId GROUP_ID = RaftGroupId.randomId();
  public static final String LOCAL_ADDR = "0.0.0.0";

  private final RaftServer server;
  private final int port;
  private final File storageDir;

  public CServer(RaftGroup group, RaftPeerId serverId, int port) throws IOException {
    this.storageDir = new File("./" + serverId);
    this.port = port;

    final RaftProperties properties = new RaftProperties();
    RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storageDir));
    RaftConfigKeys.Rpc.setType(properties, SupportedRpcType.NETTY);
    NettyConfigKeys.Server.setPort(properties, port);

    // create the counter state machine which holds the counter value.
    final CounterStateMachine counterStateMachine = new CounterStateMachine();

    // build the Raft server.
    this.server = RaftServer.newBuilder()
        .setGroup(group)
        .setProperties(properties)
        .setServerId(serverId)
        .setStateMachine(counterStateMachine)
        .setOption(RaftStorage.StartupOption.FORMAT)
        .build();
  }

  public void start() throws IOException {
    server.start();
  }

  public RaftPeer getPeer() {
    return server.getPeer();
  }

  @Override
  public void close() throws IOException {
    server.close();
    FileUtils.deleteFully(storageDir);
  }

  @Override
  public String toString() {
    try {
      return MoreObjects.toStringHelper(this)
          .add("server", server.getPeer())
          .add("role", server.getDivision(GROUP_ID).getInfo().getCurrentRole())
          .toString();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public int getPort() {
    return port;
  }
}
