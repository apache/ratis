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
package org.apache.raft.server.simulation;

import org.apache.log4j.Level;
import org.apache.raft.RaftBasicTests;
import org.apache.raft.client.RaftClient;
import org.apache.raft.conf.RaftProperties;
import org.apache.raft.server.impl.RaftServer;
import org.apache.raft.util.RaftUtils;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

public class TestRaftWithSimulatedRpc extends RaftBasicTests {
  static {
    RaftUtils.setLogLevel(RaftServer.LOG, Level.DEBUG);
    RaftUtils.setLogLevel(RaftClient.LOG, Level.DEBUG);
  }

  private final MiniRaftClusterWithSimulatedRpc cluster;

  public TestRaftWithSimulatedRpc() throws IOException {
    final RaftProperties properties = getProperties();
    if (ThreadLocalRandom.current().nextBoolean()) {
      // turn off simulate latency half of the times.
      properties.setInt(SimulatedRequestReply.SIMULATE_LATENCY_KEY, 0);
    }
    cluster = new MiniRaftClusterWithSimulatedRpc(NUM_SERVERS, properties);
  }

  @Override
  public MiniRaftClusterWithSimulatedRpc getCluster() {
    return cluster;
  }
}
