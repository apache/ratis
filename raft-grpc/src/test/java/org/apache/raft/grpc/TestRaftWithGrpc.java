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
package org.apache.raft.grpc;

import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.apache.raft.RaftBasicTests;
import org.apache.raft.grpc.server.PipelinedLogAppenderFactory;
import org.apache.raft.server.BlockRequestHandlingInjection;
import org.apache.raft.server.LogAppenderFactory;
import org.apache.raft.server.RaftServer;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.apache.raft.server.RaftServerConfigKeys.RAFT_SERVER_LOG_APPENDER_FACTORY_CLASS_KEY;

public class TestRaftWithGrpc extends RaftBasicTests {
  static {
    GenericTestUtils.setLogLevel(RaftServer.LOG, Level.DEBUG);
  }

  private final MiniRaftClusterWithGRpc cluster;

  @BeforeClass
  public static void setProp() {
    properties.setClass(RAFT_SERVER_LOG_APPENDER_FACTORY_CLASS_KEY,
        PipelinedLogAppenderFactory.class, LogAppenderFactory.class);
  }

  public TestRaftWithGrpc() throws IOException {
    cluster = new MiniRaftClusterWithGRpc(NUM_SERVERS, properties);
    Assert.assertNull(cluster.getLeader());
  }

  @Override
  public MiniRaftClusterWithGRpc getCluster() {
    return cluster;
  }

  @Override
  @Test
  public void testEnforceLeader() throws Exception {
    super.testEnforceLeader();

    MiniRaftClusterWithGRpc.sendServerRequestInjection.clear();
    BlockRequestHandlingInjection.getInstance().unblockAll();
  }

  @Override
  @Test
  public void testWithLoad() throws Exception {
    super.testWithLoad();
    BlockRequestHandlingInjection.getInstance().unblockAll();
  }
}
