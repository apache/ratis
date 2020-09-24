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
package org.apache.ratis.server.impl;

import static org.apache.ratis.RaftTestUtil.waitForLeader;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.BaseTest;
import org.apache.ratis.MiniRaftCluster;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.RaftTestUtil.SimpleMessage;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.raftlog.RaftLog;
import org.junit.Assert;
import org.junit.Test;

/** Test server pause and resume. */
public abstract class ServerPauseResumeTest <CLUSTER extends MiniRaftCluster>
    extends BaseTest
    implements MiniRaftCluster.Factory.Get<CLUSTER> {

  public static final int NUM_SERVERS = 3;

  @Test
  public void testPauseResume() throws Exception {
    runWithNewCluster(NUM_SERVERS, this::runTestPauseResume);
  }

  void runTestPauseResume(CLUSTER cluster) throws InterruptedException, IOException {
    // wait leader be elected.
    RaftServerImpl leader = waitForLeader(cluster);
    RaftPeerId leaderId = leader.getId();
    List<RaftServerImpl> followers = cluster.getFollowers();
    Assert.assertTrue(followers.size() >= 1);
    RaftServerImpl follower = followers.get(0);

    SimpleMessage[] batch1 = SimpleMessage.create(100);
    Thread writeThread = RaftTestUtil.sendMessageInNewThread(cluster, leaderId, batch1);

    writeThread.join();
    Thread.sleep(cluster.getTimeoutMax().toLong(TimeUnit.MILLISECONDS) * 5);
    RaftLog leaderLog = leader.getState().getLog();
    // leader should contain all logs.
    Assert.assertTrue(RaftTestUtil.logEntriesContains(leaderLog, batch1));
    RaftLog followerLog = follower.getState().getLog();
    // leader should contain all logs.
    Assert.assertTrue(RaftTestUtil.logEntriesContains(followerLog, batch1));

    // pause follower.
    boolean isSuccess = follower.pause();
    Assert.assertTrue(isSuccess);
    Assert.assertTrue(follower.isPausingOrPaused());

    SimpleMessage[] batch2 = SimpleMessage.create(100);
    Thread writeThread2 = RaftTestUtil.sendMessageInNewThread(cluster, leaderId, batch2);

    writeThread2.join();
    Thread.sleep(cluster.getTimeoutMax().toLong(TimeUnit.MILLISECONDS) * 5);
    // paused follower should not have any batch2 message in its raftlog.
    Assert.assertTrue(RaftTestUtil.logEntriesNotContains(followerLog, batch2));
  }

}
