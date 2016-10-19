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
import org.apache.raft.conf.RaftProperties;
import org.apache.raft.grpc.server.PipelinedLogAppenderFactory;
import org.apache.raft.server.LogAppenderFactory;
import org.apache.raft.server.RaftServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.apache.raft.RaftTestUtil.waitAndKillLeader;
import static org.apache.raft.server.RaftServerConfigKeys.RAFT_SERVER_LOG_APPENDER_FACTORY_CLASS_KEY;

public class TestRaftWithGrpc {
  static {
    GenericTestUtils.setLogLevel(RaftServer.LOG, Level.DEBUG);
  }
  private static final int NUM_PEERS = 5;
  private static RaftProperties prop;

  private MiniRaftClusterWithGRpc cluster;

  @BeforeClass
  public static void setProp() {
    prop = new RaftProperties();
    prop.setClass(RAFT_SERVER_LOG_APPENDER_FACTORY_CLASS_KEY,
        PipelinedLogAppenderFactory.class, LogAppenderFactory.class);
  }

  @Before
  public void setup() throws IOException {
    cluster = new MiniRaftClusterWithGRpc(NUM_PEERS, prop);

    Assert.assertNull(cluster.getLeader());
    cluster.start();
  }

  @After
  public void tearDown() {
    cluster.shutdown();
  }

  @Test
  public void testBasicLeaderElection() throws Exception {
    waitAndKillLeader(cluster, true);
    waitAndKillLeader(cluster, true);
    waitAndKillLeader(cluster, true);
    waitAndKillLeader(cluster, false);
  }

}
