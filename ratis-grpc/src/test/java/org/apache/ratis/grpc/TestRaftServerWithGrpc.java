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
package org.apache.ratis.grpc;

import org.apache.ratis.BaseTest;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.impl.RaftServerTestUtil;
import org.junit.Test;

import java.io.IOException;
import java.nio.channels.OverlappingFileLockException;

public class TestRaftServerWithGrpc extends BaseTest {

  @Test
  public void testServerRestartOnException() throws Exception {
    RaftProperties properties = new RaftProperties();
    final MiniRaftClusterWithGRpc cluster
        = MiniRaftClusterWithGRpc.FACTORY.newCluster(1, properties);
    cluster.start();
    RaftPeerId leaderId = RaftTestUtil.waitForLeader(cluster).getId();
    GrpcConfigKeys.Server.setPort(properties, cluster.getLeader().getServerRpc().getInetSocketAddress().getPort());
    // Create a raft server proxy with server rpc bound to a different address
    // compared to leader. This helps in locking the raft storage directory to
    // be used by next raft server proxy instance.
    RaftServerTestUtil.getRaftServerProxy(leaderId, cluster.getLeader().getStateMachine(), cluster.getGroup(),
        new RaftProperties(), null);
    // Close the server rpc for leader so that new raft server can be bound to it.
    cluster.getLeader().getServerRpc().close();

    // Create a raft server proxy with server rpc bound to same address as
    // the leader. This step would fail as the raft storage has been locked by
    // the raft server proxy created earlier. Raft server proxy should close
    // the rpc server on failure.
    testFailureCase("start a new server with the same address",
        () -> RaftServerTestUtil.getRaftServerProxy(leaderId, cluster.getLeader().getStateMachine(),
            cluster.getGroup(), properties, null),
        IOException.class, OverlappingFileLockException.class);
    // Try to start a raft server rpc at the leader address.
    cluster.getServer(leaderId).getFactory().newRaftServerRpc(cluster.getServer(leaderId));
  }
}
