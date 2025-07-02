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

import org.apache.ratis.BaseTest;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.grpc.MiniRaftClusterWithGrpc;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.TimeDuration;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test StateMachine related functionality
 */
public class TestManualRestoreSnapshot extends BaseTest implements MiniRaftClusterWithGrpc.FactoryGet {
  public static final int NUM_SERVERS = 3;

  {
    getProperties().setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY, MonotonicStateMachine.class, StateMachine.class);
  }

  @Test
  public void testManualRestoreSnapshot() throws Exception {
    runWithNewCluster(NUM_SERVERS, this::run);
  }

  void run(MiniRaftCluster cluster) throws Exception {
    final RaftGroup group = cluster.getGroup();

    // send some messages
    final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster);
    LOG.info("Leader: {}", leader);
    sendMessages(cluster, 0, 5);

    // kill a follower
    final RaftServer.Division toBeKilled = cluster.getFollowers().get(0);
    LOG.info("Follower to be killed: {}", toBeKilled.getId());
    final SimpleStateMachineStorage smStorage = ((MonotonicStateMachine) toBeKilled.getStateMachine()).getStorage();
    final File raftLogCurrentDir = toBeKilled.getRaftStorage().getStorageDir().getCurrentDir();
    cluster.killServer(toBeKilled.getId());

    // send more messages
    sendMessages(cluster, 5, 3);

    // get a snapshot from the leader
    final MonotonicStateMachine leaderStateMachine = (MonotonicStateMachine) leader.getStateMachine();
    final MonotonicStateMachine.Snapshot snapshot = leaderStateMachine.getSnapshot();
    LOG.info("{}: Leader {}", leader.getId(), snapshot);

    // remove raft log from the killed follower
    FileUtils.listDir(raftLogCurrentDir, s -> LOG.info("{}", s), LOG::error);
    final String[] logFiles = raftLogCurrentDir.list((dir, name) -> name.startsWith("log"));
    assertNotNull(logFiles);
    for (String logFile : logFiles) {
      FileUtils.deleteFile(new File(raftLogCurrentDir, logFile));
    }

    // remove the killed follower
    final RaftPeerId followerId = toBeKilled.getId();
    cluster.removeServer(followerId);

    // save the leader snapshot to the killed follower
    final TermIndex applied = snapshot.getApplied();
    final File snapshotFile = smStorage.getSnapshotFile(applied.getTerm(), applied.getIndex());
    final RaftServer toSaveSnapshot = cluster.putNewServer(followerId, group, false);
    ((MonotonicStateMachine) toSaveSnapshot.getDivision(group.getGroupId()).getStateMachine())
        .saveSnapshot(snapshot, snapshotFile);

    // start follower and verify last applied
    LOG.info("Restarting {}", followerId);
    final RaftServer.Division restartedFollower = cluster.restartServer(followerId, group, false);
    final StateMachine stateMachine = restartedFollower.getStateMachine();
    final SnapshotInfo info = stateMachine.getLatestSnapshot();
    LOG.info("{} restarted snapshot info {} from {}", followerId, info, stateMachine);

    JavaUtils.attemptUntilTrue(() -> {
      System.out.println(cluster.printServers());
      final TermIndex leaderLastApplied = leaderStateMachine.getLastAppliedTermIndex();
      LOG.info("Leader   {} last applied {}", leader.getId(), leaderLastApplied);
      final TermIndex followerLastApplied = stateMachine.getLastAppliedTermIndex();
      LOG.info("Follower {} last applied {}", followerId, followerLastApplied);
      return followerLastApplied.equals(leaderLastApplied);
    }, 10, TimeDuration.ONE_SECOND, "followerLastApplied", LOG);

    sendMessages(cluster, 8, 7);
  }

  static void sendMessages(MiniRaftCluster cluster, int base, int numMessages) throws Exception {
    final List<Message> messages = MonotonicStateMachine.getUpdateRequests(base, numMessages);
    try(final RaftClient client = cluster.createClient()) {
      for (Message message : messages) {
        final RaftClientReply reply = client.io().send(message);
        assertTrue(reply.isSuccess());
      }
    }
  }
}
