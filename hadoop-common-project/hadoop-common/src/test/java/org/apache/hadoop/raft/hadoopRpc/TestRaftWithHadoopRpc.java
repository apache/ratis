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
import org.apache.hadoop.raft.MiniRaftCluster;
import org.apache.hadoop.raft.conf.RaftProperties;
import org.apache.hadoop.raft.server.RaftServer;
import org.apache.hadoop.raft.server.RaftServerConfigKeys;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.hadoop.raft.RaftTestUtil.waitAndKillLeader;

public class TestRaftWithHadoopRpc {
  static final Logger LOG = LoggerFactory.getLogger(TestRaftWithHadoopRpc.class);

  static {
    GenericTestUtils.setLogLevel(RaftServer.LOG, Level.DEBUG);
  }

  private final Configuration conf = new Configuration();
  private MiniRaftCluster cluster;

  @Before
  public void setup() throws IOException {
    final RaftProperties prop = new RaftProperties();
    prop.setBoolean(RaftServerConfigKeys.RAFT_SERVER_USE_MEMORY_LOG_KEY, true);

    conf.set(RaftServerConfigKeys.Ipc.ADDRESS_KEY, "0.0.0.0:0");

    cluster = new MiniRaftClusterWithHadoopRpc(5, prop, conf);
    Assert.assertNull(cluster.getLeader());
    cluster.start();
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testBasicLeaderElection() throws Exception {
    waitAndKillLeader(cluster, true);
    waitAndKillLeader(cluster, true);
    waitAndKillLeader(cluster, true);
    waitAndKillLeader(cluster, false);
  }
}
