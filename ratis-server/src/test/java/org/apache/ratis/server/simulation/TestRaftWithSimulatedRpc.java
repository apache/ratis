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
package org.apache.ratis.server.simulation;

import org.apache.log4j.Level;
import org.apache.ratis.RaftBasicTests;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.util.LogUtils;

import java.io.IOException;

public class TestRaftWithSimulatedRpc extends RaftBasicTests {
  static {
    LogUtils.setLogLevel(RaftServerImpl.LOG, Level.DEBUG);
    LogUtils.setLogLevel(RaftClient.LOG, Level.DEBUG);
  }

  private final MiniRaftClusterWithSimulatedRpc cluster;

  public TestRaftWithSimulatedRpc() throws IOException {
    cluster = MiniRaftClusterWithSimulatedRpc.FACTORY.newCluster(
        NUM_SERVERS, getProperties());
  }

  @Override
  public MiniRaftClusterWithSimulatedRpc getCluster() {
    return cluster;
  }
}
