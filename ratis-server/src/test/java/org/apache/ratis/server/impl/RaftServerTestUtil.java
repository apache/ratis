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

import org.apache.ratis.MiniRaftCluster;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.impl.RaftConfiguration;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.statemachine.StateMachine;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class RaftServerTestUtil {
  static final Logger LOG = LoggerFactory.getLogger(RaftServerTestUtil.class);

  public static void waitAndCheckNewConf(MiniRaftCluster cluster,
      RaftPeer[] peers, int numOfRemovedPeers, Collection<String> deadPeers)
      throws Exception {
    final long sleepMs = cluster.getMaxTimeout() * (numOfRemovedPeers + 2);
    RaftTestUtil.attempt(3, sleepMs,
        () -> waitAndCheckNewConf(cluster, peers, deadPeers));
  }
  private static void waitAndCheckNewConf(MiniRaftCluster cluster,
      RaftPeer[] peers, Collection<String> deadPeers)
      throws Exception {
    LOG.info(cluster.printServers());
    Assert.assertNotNull(cluster.getLeader());

    int numIncluded = 0;
    int deadIncluded = 0;
    final RaftConfiguration current = RaftConfiguration.newBuilder()
        .setConf(peers).setLogEntryIndex(0).build();
    for (RaftServerImpl server : cluster.getServers()) {
      if (deadPeers != null && deadPeers.contains(server.getId())) {
        if (current.containsInConf(server.getId())) {
          deadIncluded++;
        }
        continue;
      }
      if (current.containsInConf(server.getId())) {
        numIncluded++;
        Assert.assertTrue(server.getRaftConf().isStable());
        Assert.assertTrue(server.getRaftConf().hasNoChange(peers));
      } else {
        Assert.assertFalse(server.getId() + " is still running: " + server,
            server.isAlive());
      }
    }
    Assert.assertEquals(peers.length, numIncluded + deadIncluded);
  }

  public static StateMachine getStateMachine(RaftServerImpl s) {
    return s.getStateMachine();
  }
}
