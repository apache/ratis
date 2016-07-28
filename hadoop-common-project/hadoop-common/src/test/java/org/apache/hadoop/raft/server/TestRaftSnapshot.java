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
package org.apache.hadoop.raft.server;

import org.apache.hadoop.raft.MiniRaftCluster;
import org.apache.hadoop.raft.client.RaftClient;
import org.apache.hadoop.raft.conf.RaftProperties;
import org.apache.hadoop.raft.server.simulation.MiniRaftClusterWithSimulatedRpc;
import org.apache.hadoop.raft.server.simulation.RequestHandler;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class TestRaftSnapshot {
  static final Logger LOG = LoggerFactory.getLogger(TestRaftSnapshot.class);

  static {
    GenericTestUtils.setLogLevel(RaftServer.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(RequestHandler.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(RaftClient.LOG, Level.DEBUG);
  }

  private MiniRaftCluster cluster;

  @Before
  public void setup() {
    RaftProperties prop = new RaftProperties();
    prop.setClass(RaftServerConfigKeys.RAFT_SERVER_STATEMACHINE_CLASS_KEY,
        SimpleStateMachine.class, StateMachine.class);
    prop.setLong(RaftServerConfigKeys.RAFT_SERVER_SNAPSHOT_TRIGGER_THRESHOLD_KEY, 100);
    cluster = new MiniRaftClusterWithSimulatedRpc(5, prop);
    cluster.start();
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Keep generating writing traffic and make sure snapshots are taken.
   * We then restart the whole raft peer and check if it can correctly load
   * snapshots + raft log.
   */
  @Test
  public void testRestart() throws IOException {

  }
}
