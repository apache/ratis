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
package org.apache.hadoop.raft.server.simulation;

import org.apache.hadoop.raft.RaftBasicTests;
import org.apache.hadoop.raft.client.RaftClient;
import org.apache.hadoop.raft.server.RaftServer;
import org.apache.hadoop.raft.server.storage.SegmentedRaftLog;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;

import java.io.IOException;

public class TestRaftWithSimulatedRpc extends RaftBasicTests {
  static {
    GenericTestUtils.setLogLevel(RaftServer.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(SegmentedRaftLog.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(RequestHandler.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(RaftClient.LOG, Level.DEBUG);
  }

  private final MiniRaftClusterWithSimulatedRpc cluster;

  public TestRaftWithSimulatedRpc() throws IOException {
    cluster = new MiniRaftClusterWithSimulatedRpc(NUM_SERVERS, getProperties());
  }

  @Override
  public MiniRaftClusterWithSimulatedRpc getCluster() {
    return cluster;
  }
}
