/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.ratis.server.impl;

import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.ratis.BaseTest;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.raftlog.RaftLog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public abstract class LogMetadataTests<CLUSTER extends MiniRaftCluster>
    extends BaseTest
    implements MiniRaftCluster.Factory.Get<CLUSTER> {

  @Test
  public void testLogMetadataEnabled() throws Exception {
    testLogMetadataBasicTest(true, x -> x > RaftLog.INVALID_LOG_INDEX);
  }

  @Test
  public void testLogMetadataDisabled() throws Exception {
    testLogMetadataBasicTest(false, x -> x == RaftLog.INVALID_LOG_INDEX);
  }

  public void testLogMetadataBasicTest(boolean logMetadata, Predicate<Long> checker)
      throws Exception {
    final RaftProperties prop = getProperties();
    RaftServerConfigKeys.Log.setLogMetadataEnabled(prop, logMetadata);

    final MiniRaftCluster cluster = newCluster(3);
    try {
      cluster.start();
      RaftTestUtil.waitForLeader(cluster);
      final RaftServer.Division leader = cluster.getLeader();
      RaftPeerId leaderId = leader.getId();

      cluster.getLeaderAndSendFirstMessage(true);

      // kill majority servers
      for (RaftPeerId id : cluster.getGroup().getPeers().stream().map(RaftPeer::getId)
          .filter(x -> !x.equals(leaderId)).collect(Collectors.toList())) {
        cluster.killServer(id);
      }

      // only restart one server
      cluster.restartServer(leaderId, false);

      long commitIndex = cluster.getServer(leaderId).getDivision(cluster.getGroupId()).getRaftLog()
          .getLastCommittedIndex();

      Assertions.assertTrue(checker.test(commitIndex));
    } finally {
      cluster.shutdown();
    }
  }
}
