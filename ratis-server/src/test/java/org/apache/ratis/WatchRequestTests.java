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
package org.apache.ratis;

import org.apache.log4j.Level;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.CommitInfoProto;
import org.apache.ratis.proto.RaftProtos.ReplicationLevel;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.statemachine.SimpleStateMachine4Testing;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.LogUtils;
import org.apache.ratis.util.TimeDuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public abstract class WatchRequestTests<CLUSTER extends MiniRaftCluster>
    extends BaseTest
    implements MiniRaftCluster.Factory.Get<CLUSTER> {
  static {
    LogUtils.setLogLevel(RaftServerImpl.LOG, Level.DEBUG);
    LogUtils.setLogLevel(RaftClient.LOG, Level.DEBUG);
  }

  static final int NUM_SERVERS = 3;
  static final int GET_TIMEOUT_SECOND = 5;

  @Before
  public void setup() {
    final RaftProperties p = getProperties();
    p.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        SimpleStateMachine4Testing.class, StateMachine.class);
    RaftClientConfigKeys.Rpc.setRetryInterval(p, TimeDuration.valueOf(100, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testWatchRequestAsync() throws Exception {
    LOG.info("Running testWatchRequests");
    try(final CLUSTER cluster = newCluster(NUM_SERVERS)) {
      cluster.start();
      runTestWatchRequestAsync(cluster, LOG);
    }
  }

  static void runTestWatchRequestAsync(MiniRaftCluster cluster, Logger LOG) throws Exception {
    try(final RaftClient writeClient = cluster.createClient(RaftTestUtil.waitForLeader(cluster).getId());
        final RaftClient watchMajorityClient = cluster.createClient(RaftTestUtil.waitForLeader(cluster).getId());
        final RaftClient watchAllClient = cluster.createClient(RaftTestUtil.waitForLeader(cluster).getId());
        final RaftClient watchAllCommittedClient = cluster.createClient(RaftTestUtil.waitForLeader(cluster).getId())) {
      long logIndex;
      {
        // send the first message
        final RaftTestUtil.SimpleMessage message = new RaftTestUtil.SimpleMessage("message");
        final RaftClientReply reply = writeClient.sendAsync(message).get(GET_TIMEOUT_SECOND, TimeUnit.SECONDS);
        Assert.assertTrue(reply.isSuccess());
        logIndex = reply.getLogIndex();

        final List<CompletableFuture<Void>> futures = new ArrayList<>();
        futures.add(watchMajorityClient.sendWatchAsync(logIndex, ReplicationLevel.MAJORITY)
            .thenAccept(r -> Assert.assertTrue(r.isSuccess())));
        futures.add(watchAllClient.sendWatchAsync(logIndex, ReplicationLevel.ALL)
            .thenAccept(r -> Assert.assertTrue(r.isSuccess())));
        futures.add(watchAllCommittedClient.sendWatchAsync(logIndex, ReplicationLevel.ALL_COMMITTED)
            .thenAccept(r -> Assert.assertTrue(r.isSuccess())));
        JavaUtils.allOf(futures).get(GET_TIMEOUT_SECOND, TimeUnit.SECONDS);
      }
      logIndex++;

      for(int i = 0; i < 5; i++) {
        final int numMessages = ThreadLocalRandom.current().nextInt(10) + 1;
        runTestWatchRequestAsync(logIndex, numMessages, writeClient, watchMajorityClient, watchAllClient, watchAllCommittedClient, cluster, LOG);
        logIndex += numMessages;
      }

      LOG.info(cluster.printServers());
    }
  }

  static void runTestWatchRequestAsync(long startLogIndex, int numMessages,
      RaftClient writeClient, RaftClient watchMajorityClient, RaftClient watchAllClient, RaftClient watchAllCommittedClient,
      MiniRaftCluster cluster, Logger LOG) throws Exception {
    LOG.info("runTestWatchRequestAsync: startLogIndex={}, numMessages={}", startLogIndex, numMessages);

    // blockStartTransaction of the leader so that no transaction can be committed MAJORITY
    final RaftServerImpl leader = cluster.getLeader();
    LOG.info("block leader {}", leader.getId());
    SimpleStateMachine4Testing.get(leader).blockStartTransaction();

    // blockFlushStateMachineData a follower so that no transaction can be ALL_COMMITTED
    final List<RaftServerImpl> followers = cluster.getFollowers();
    final RaftServerImpl blockedFollower = followers.get(ThreadLocalRandom.current().nextInt(followers.size()));
    LOG.info("block follower {}", blockedFollower.getId());
    SimpleStateMachine4Testing.get(blockedFollower).blockFlushStateMachineData();

    // send a message
    final List<CompletableFuture<RaftClientReply>> replies = new ArrayList<>();
    final List<CompletableFuture<RaftClientReply>> watchMajoritys = new ArrayList<>();
    final List<CompletableFuture<RaftClientReply>> watchAlls = new ArrayList<>();
    final List<CompletableFuture<RaftClientReply>> watchAllCommitteds = new ArrayList<>();

    for(int i = 0; i < numMessages; i++) {
      final long logIndex = startLogIndex + i;
      final String message = "m" + logIndex;
      LOG.info("SEND_REQUEST {}: logIndex={}, message={}", i, logIndex, message);
      replies.add(writeClient.sendAsync(new RaftTestUtil.SimpleMessage(message)));
      watchMajoritys.add(watchMajorityClient.sendWatchAsync(logIndex, ReplicationLevel.MAJORITY));
      watchAlls.add(watchAllClient.sendWatchAsync(logIndex, ReplicationLevel.ALL));
      watchAllCommitteds.add(watchAllCommittedClient.sendWatchAsync(logIndex, ReplicationLevel.ALL_COMMITTED));
    }

    Assert.assertEquals(numMessages, replies.size());
    Assert.assertEquals(numMessages, watchMajoritys.size());
    Assert.assertEquals(numMessages, watchAlls.size());
    Assert.assertEquals(numMessages, watchAllCommitteds.size());

    // since leader is blocked, nothing can be done.
    TimeUnit.SECONDS.sleep(1);
    assertNotDone(replies);
    assertNotDone(watchMajoritys);
    assertNotDone(watchAlls);
    assertNotDone(watchAllCommitteds);

    // unblock leader so that the transaction can be committed.
    SimpleStateMachine4Testing.get(leader).unblockStartTransaction();
    LOG.info("unblock leader {}", leader.getId());
    for(int i = 0; i < numMessages; i++) {
      final long logIndex = startLogIndex + i;
      LOG.info("UNBLOCK_LEADER {}: logIndex={}", i, logIndex);
      final RaftClientReply reply = replies.get(i).get(GET_TIMEOUT_SECOND, TimeUnit.SECONDS);
      Assert.assertTrue(reply.isSuccess());
      Assert.assertEquals(logIndex, reply.getLogIndex());
      final RaftClientReply watchMajorityReply = watchMajoritys.get(i).get(GET_TIMEOUT_SECOND, TimeUnit.SECONDS);
      LOG.info("watchMajorityReply({}) = {}", logIndex, watchMajorityReply);
      Assert.assertTrue(watchMajoritys.get(i).get().isSuccess());
    }
    // but not replicated/committed to all.
    TimeUnit.SECONDS.sleep(1);
    assertNotDone(watchAlls);
    assertNotDone(watchAllCommitteds);

    // unblock follower so that the transaction can be replicated and committed to all.
    LOG.info("unblock follower {}", blockedFollower.getId());
    SimpleStateMachine4Testing.get(blockedFollower).unblockFlushStateMachineData();
    for(int i = 0; i < numMessages; i++) {
      final long logIndex = startLogIndex + i;
      LOG.info("UNBLOCK_FOLLOWER {}: logIndex={}", i, logIndex);
      final RaftClientReply watchAllReply = watchAlls.get(i).get(GET_TIMEOUT_SECOND, TimeUnit.SECONDS);
      LOG.info("watchAllReply({}) = {}", logIndex, watchAllReply);
      Assert.assertTrue(watchAllReply.isSuccess());

      final RaftClientReply watchAllCommittedReply = watchAllCommitteds.get(i).get(GET_TIMEOUT_SECOND, TimeUnit.SECONDS);
      LOG.info("watchAllCommittedReply({}) = ", logIndex, watchAllCommittedReply);
      Assert.assertTrue(watchAllCommittedReply.isSuccess());
      { // check commit infos
        final Collection<CommitInfoProto> commitInfos = watchAllCommittedReply.getCommitInfos();
        Assert.assertEquals(NUM_SERVERS, commitInfos.size());
        commitInfos.forEach(info -> Assert.assertTrue(logIndex <= info.getCommitIndex()));
      }
    }
  }

  static <T> void assertNotDone(List<CompletableFuture<T>> futures) {
    futures.forEach(f -> Assert.assertFalse(f.isDone()));
  }
}
