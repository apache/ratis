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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ratis.statemachine;

import org.apache.log4j.Level;
import org.apache.ratis.BaseTest;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.SnapshotRequest;
import org.apache.ratis.rpc.CallId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.server.impl.RaftServerTestUtil;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Log4jUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public abstract class RaftTakeSnapshotTest extends BaseTest {

  {
    Log4jUtils.setLogLevel(RaftServer.Division.LOG, Level.DEBUG);
    Log4jUtils.setLogLevel(RaftLog.LOG, Level.INFO);
    Log4jUtils.setLogLevel(RaftClient.LOG, Level.INFO);
  }

  static final Logger LOG = LoggerFactory.getLogger(RaftTakeSnapshotTest.class);
  private MiniRaftCluster cluster;

  public MiniRaftCluster.Factory<?> getFactory() {
    return null;
  }

  @Test
  public void testTakeSnapshot() throws Exception {
    final RaftProperties prop = new RaftProperties();
    prop.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
          SimpleStateMachine4Testing.class, StateMachine.class);
    RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(prop, false);
    cluster = getFactory().newCluster(1,prop);
    cluster.start();

    int i = 0;
    try {
      final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster);
      final RaftPeerId leaderId = leader.getId();
      try(final RaftClient client = cluster.createClient(leaderId)) {
        //todo(yaolong liu) : make 5 to be a configurable value
        for (; i < 5; i++) {
          RaftClientReply reply = client.io().send(new RaftTestUtil.SimpleMessage("m" + i));
          Assert.assertTrue(reply.isSuccess());
        }
        final SnapshotRequest r = new SnapshotRequest(client.getId(), leaderId, cluster.getGroupId(),
              CallId.getAndIncrement(), 3000);
        RaftServerTestUtil.takeSnapshotAsync(leader, r);
      }

      // wait for the snapshot to be done
      long nextIndex = cluster.getLeader().getRaftLog().getNextIndex();
      LOG.info("nextIndex = {}", nextIndex);

      final List<File> snapshotFiles = LongStream.range(0, nextIndex)
            .mapToObj(j ->
                 SimpleStateMachine4Testing
                        .get(cluster.getLeader())
                        .getStateMachineStorage()
                        .getSnapshotFile(cluster.getLeader().getInfo().getCurrentTerm(), j))
            .collect(Collectors.toList());
      JavaUtils.attemptRepeatedly(() -> {
        Assert.assertTrue(snapshotFiles.stream().anyMatch(File::exists));
        return null;
      }, 100, ONE_SECOND, "snapshotFile.exist", LOG);
    } finally {
      cluster.shutdown();
    }
  }

}
