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
package org.apache.hadoop.raft.hadoopRpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.raft.RaftBasicTests;
import org.apache.hadoop.raft.server.RaftServer;
import org.apache.hadoop.raft.server.RaftServerConfigKeys;
import org.apache.hadoop.raft.util.BlockReceiveServerRequest;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.Test;

import java.io.IOException;

public class TestRaftWithHadoopRpc extends RaftBasicTests {
  static {
    GenericTestUtils.setLogLevel(RaftServer.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(MiniRaftClusterWithHadoopRpc.LOG, Level.DEBUG);
  }

  private final Configuration conf = new Configuration();
  private final MiniRaftClusterWithHadoopRpc cluster;

  public TestRaftWithHadoopRpc() throws IOException {
    conf.set(RaftServerConfigKeys.Ipc.ADDRESS_KEY, "0.0.0.0:0");
    cluster = new MiniRaftClusterWithHadoopRpc(NUM_SERVERS, getProperties(), conf);
  }

  @Override
  public MiniRaftClusterWithHadoopRpc getCluster() {
    return cluster;
  }

  @Override
  @Test
  public void testEnforceLeader() throws Exception {
    super.testEnforceLeader();

    DelaySendServerRequest.clear();
    BlockReceiveServerRequest.clear();
  }
}
