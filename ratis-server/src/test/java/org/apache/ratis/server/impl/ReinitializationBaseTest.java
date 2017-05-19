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
package org.apache.ratis.server.impl;

import org.apache.log4j.Level;
import org.apache.ratis.MiniRaftCluster;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.util.LogUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public abstract class ReinitializationBaseTest {
  static {
    LogUtils.setLogLevel(RaftServerImpl.LOG, Level.DEBUG);
    LogUtils.setLogLevel(RaftClient.LOG, Level.TRACE);
  }
  static final Logger LOG = LoggerFactory.getLogger(ReinitializationBaseTest.class);

  static final RaftProperties prop = new RaftProperties();

  public abstract MiniRaftCluster.Factory<? extends MiniRaftCluster> getClusterFactory();

  public MiniRaftCluster getCluster(int peerNum) throws IOException {
    return getClusterFactory().newCluster(peerNum, prop);
  }

  @Test
  public void testReinitialize() throws Exception {
    final MiniRaftCluster cluster = getCluster(0);
    LOG.info("Start testReinitialize" + cluster.printServers());

    // Start server with an empty conf
    final RaftConfiguration emptyConf = MiniRaftCluster.initConfiguration(Collections.emptyList());

    final List<RaftPeerId> ids = Arrays.asList(MiniRaftCluster.generateIds(3, 0))
        .stream().map(RaftPeerId::valueOf).collect(Collectors.toList());
    ids.stream().forEach(id -> cluster.putNewServer(id, emptyConf, true));
    LOG.info("putNewServer: " + cluster.printServers());

    cluster.start();
    LOG.info("start: " + cluster.printServers());

    // Make sure that there are no leaders.
    TimeUnit.SECONDS.sleep(1);
    Assert.assertNull(cluster.getLeader());

    // Reinitialize servers
    final RaftPeer[] peers = cluster.getPeers().toArray(RaftPeer.EMPTY_PEERS);
    for(RaftPeer p : peers) {
      final RaftClient client = cluster.createClient(p.getId(), new ArrayList<>(Arrays.asList(p)));
      client.reinitialize(peers, p.getId());
    }
    Assert.assertNotNull(RaftTestUtil.waitForLeader(cluster, true));
  }
}
