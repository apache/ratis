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
package org.apache.ratis;

import org.apache.log4j.Level;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.CommitInfoProto;
import org.apache.ratis.proto.RaftProtos.ReplicationLevel;
import org.apache.ratis.protocol.NotReplicatedException;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.impl.RaftServerTestUtil;
import org.apache.ratis.statemachine.SimpleStateMachine4Testing;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.LogUtils;
import org.apache.ratis.util.ProtoUtils;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.function.CheckedConsumer;
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
import java.util.stream.Stream;

public abstract class WatchRequestTests<CLUSTER extends MiniRaftCluster>
    extends BaseTest
    implements MiniRaftCluster.Factory.Get<CLUSTER> {
  {
    RaftServerTestUtil.setWatchRequestsLogLevel(Level.DEBUG);
    LogUtils.setLogLevel(RaftServerImpl.LOG, Level.DEBUG);
  }

  static final int NUM_SERVERS = 3;
  static final int GET_TIMEOUT_SECOND = 5;

  @Before
  public void setup() {
    final RaftProperties p = getProperties();
    p.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        SimpleStateMachine4Testing.class, StateMachine.class);
  }

  @Test
  public void testWatchRequestAsync() throws Exception {
    runWithNewCluster(NUM_SERVERS, cluster -> runTest(WatchRequestTests::runTestWatchRequestAsync, cluster, LOG));
  }

  static class TestParameters {
    final int numMessages;
    final RaftClient writeClient;
    final RaftClient watchMajorityClient;
    final RaftClient watchAllClient;
    final RaftClient watchMajorityCommittedClient;
    final RaftClient watchAllCommittedClient;
    final MiniRaftCluster cluster;
    final Logger log;

    TestParameters(int numMessages, RaftClient writeClient,
        RaftClient watchMajorityClient, RaftClient watchAllClient,
        RaftClient watchMajorityCommittedClient, RaftClient watchAllCommittedClient,
        MiniRaftCluster cluster, Logger log) {
      this.numMessages = numMessages;
      this.writeClient = writeClient;
      this.watchMajorityClient = watchMajorityClient;
      this.watchAllClient = watchAllClient;
      this.watchMajorityCommittedClient = watchMajorityCommittedClient;
      this.watchAllCommittedClient = watchAllCommittedClient;
      this.cluster = cluster;
      this.log = log;
    }

    void sendRequests(List<CompletableFuture<RaftClientReply>> replies,
        List<CompletableFuture<WatchReplies>> watches) {
      for(int i = 0; i < numMessages; i++) {
        final String message = "m" + i;
        log.info("SEND_REQUEST {}: message={}", i, message);
        final CompletableFuture<RaftClientReply> replyFuture = writeClient.sendAsync(new RaftTestUtil.SimpleMessage(message));
        replies.add(replyFuture);
        final CompletableFuture<WatchReplies> watchFuture = new CompletableFuture<>();
        watches.add(watchFuture);
        replyFuture.thenAccept(reply -> {
          final long logIndex = reply.getLogIndex();
          log.info("SEND_WATCH: message={}, logIndex={}", message, logIndex);
          watchFuture.complete(new WatchReplies(logIndex,
              watchMajorityClient.sendWatchAsync(logIndex, ReplicationLevel.MAJORITY),
              watchAllClient.sendWatchAsync(logIndex, ReplicationLevel.ALL),
              watchMajorityCommittedClient.sendWatchAsync(logIndex, ReplicationLevel.MAJORITY_COMMITTED),
              watchAllCommittedClient.sendWatchAsync(logIndex, ReplicationLevel.ALL_COMMITTED),
              log));
        });
      }
    }

    @Override
    public String toString() {
      return "numMessages=" + numMessages;
    }
  }

  static void runTest(CheckedConsumer<TestParameters, Exception> testCase, MiniRaftCluster cluster, Logger LOG) throws Exception {
    try(final RaftClient writeClient = cluster.createClient(RaftTestUtil.waitForLeader(cluster).getId());
        final RaftClient watchMajorityClient = cluster.createClient(RaftTestUtil.waitForLeader(cluster).getId());
        final RaftClient watchAllClient = cluster.createClient(RaftTestUtil.waitForLeader(cluster).getId());
        final RaftClient watchMajorityCommittedClient = cluster.createClient(RaftTestUtil.waitForLeader(cluster).getId());
        final RaftClient watchAllCommittedClient = cluster.createClient(RaftTestUtil.waitForLeader(cluster).getId())) {
      final int[] numMessages = {1, 10, 100};
      for(int i = 0; i < 5; i++) {
        final int n = numMessages[ThreadLocalRandom.current().nextInt(numMessages.length)];
        final TestParameters p = new TestParameters(
            n, writeClient, watchMajorityClient, watchAllClient,
            watchMajorityCommittedClient, watchAllCommittedClient, cluster, LOG);
        LOG.info("{}) {}, {}", i, p, cluster.printServers());
        testCase.accept(p);
      }
    }
  }

  static class WatchReplies {
    private final long logIndex;
    private final CompletableFuture<RaftClientReply> majority;
    private final CompletableFuture<RaftClientReply> all;
    private final CompletableFuture<RaftClientReply> majorityCommitted;
    private final CompletableFuture<RaftClientReply> allCommitted;
    private final Logger log;

    WatchReplies(long logIndex,
        CompletableFuture<RaftClientReply> majority, CompletableFuture<RaftClientReply> all,
        CompletableFuture<RaftClientReply> majorityCommitted, CompletableFuture<RaftClientReply> allCommitted, Logger log) {
      this.logIndex = logIndex;
      this.majority = majority;
      this.all = all;
      this.majorityCommitted = majorityCommitted;
      this.allCommitted = allCommitted;
      this.log = log;
    }

    RaftClientReply getMajority() throws Exception {
      final RaftClientReply reply = majority.get(GET_TIMEOUT_SECOND, TimeUnit.SECONDS);
      log.info("watchMajorityReply({}) = {}", logIndex, reply);
      return reply;
    }

    RaftClientReply getMajorityCommitted() throws Exception {
      final RaftClientReply reply = majorityCommitted.get(GET_TIMEOUT_SECOND, TimeUnit.SECONDS);
      log.info("watchMajorityCommittedReply({}) = {}", logIndex, reply);
      return reply;
    }

    RaftClientReply getAll() throws Exception {
      final RaftClientReply reply = all.get(GET_TIMEOUT_SECOND, TimeUnit.SECONDS);
      log.info("watchAllReply({}) = {}", logIndex, reply);
      return reply;
    }

    RaftClientReply getAllCommitted() throws Exception {
      final RaftClientReply reply = allCommitted.get(GET_TIMEOUT_SECOND, TimeUnit.SECONDS);
      log.info("watchAllCommittedReply({}) = {}", logIndex, reply);
      return reply;
    }
  }

  static void runTestWatchRequestAsync(TestParameters p) throws Exception {
    final Logger LOG = p.log;
    final MiniRaftCluster cluster = p.cluster;
    final int numMessages = p.numMessages;

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
    final List<CompletableFuture<WatchReplies>> watches = new ArrayList<>();

    p.sendRequests(replies, watches);

    Assert.assertEquals(numMessages, replies.size());
    Assert.assertEquals(numMessages, watches.size());

    // since leader is blocked, nothing can be done.
    TimeUnit.SECONDS.sleep(1);
    assertNotDone(replies);
    assertNotDone(watches);

    // unblock leader so that the transaction can be committed.
    SimpleStateMachine4Testing.get(leader).unblockStartTransaction();
    LOG.info("unblock leader {}", leader.getId());

    checkMajority(replies, watches, LOG);

    Assert.assertEquals(numMessages, watches.size());

    // but not replicated/committed to all.
    TimeUnit.SECONDS.sleep(1);
    assertNotDone(watches.stream().map(CompletableFuture::join).map(w -> w.all));
    assertNotDone(watches.stream().map(CompletableFuture::join).map(w -> w.allCommitted));

    // unblock follower so that the transaction can be replicated and committed to all.
    LOG.info("unblock follower {}", blockedFollower.getId());
    SimpleStateMachine4Testing.get(blockedFollower).unblockFlushStateMachineData();
    checkAll(watches, LOG);
  }

  static void checkMajority(List<CompletableFuture<RaftClientReply>> replies,
      List<CompletableFuture<WatchReplies>> watches, Logger LOG) throws Exception {
    for(int i = 0; i < replies.size(); i++) {
      final RaftClientReply reply = replies.get(i).get(GET_TIMEOUT_SECOND, TimeUnit.SECONDS);
      LOG.info("checkMajority {}: receive {}", i, reply);
      final long logIndex = reply.getLogIndex();
      Assert.assertTrue(reply.isSuccess());

      final WatchReplies watchReplies = watches.get(i).get(GET_TIMEOUT_SECOND, TimeUnit.SECONDS);
      Assert.assertEquals(logIndex, watchReplies.logIndex);
      final RaftClientReply watchMajorityReply = watchReplies.getMajority();
      Assert.assertTrue(watchMajorityReply.isSuccess());

      final RaftClientReply watchMajorityCommittedReply = watchReplies.getMajorityCommitted();
      Assert.assertTrue(watchMajorityCommittedReply.isSuccess());
      { // check commit infos
        final Collection<CommitInfoProto> commitInfos = watchMajorityCommittedReply.getCommitInfos();
        final String message = "logIndex=" + logIndex + ", " + ProtoUtils.toString(commitInfos);
        Assert.assertEquals(NUM_SERVERS, commitInfos.size());

        // One follower has not committed, so min must be less than logIndex
        final long min = commitInfos.stream().map(CommitInfoProto::getCommitIndex).min(Long::compare).get();
        Assert.assertTrue(message, logIndex > min);

        // All other followers have committed
        commitInfos.stream()
            .map(CommitInfoProto::getCommitIndex).sorted(Long::compare)
            .skip(1).forEach(ci -> Assert.assertTrue(message, logIndex <= ci));
      }
    }
  }

  static void checkAll(List<CompletableFuture<WatchReplies>> watches, Logger LOG) throws Exception {
    for(int i = 0; i < watches.size(); i++) {
      final WatchReplies watchReplies = watches.get(i).get(GET_TIMEOUT_SECOND, TimeUnit.SECONDS);
      final long logIndex = watchReplies.logIndex;
      LOG.info("checkAll {}: logIndex={}", i, logIndex);
      final RaftClientReply watchAllReply = watchReplies.getAll();
      Assert.assertTrue(watchAllReply.isSuccess());

      final RaftClientReply watchAllCommittedReply = watchReplies.getAllCommitted();
      Assert.assertTrue(watchAllCommittedReply.isSuccess());
      { // check commit infos
        final Collection<CommitInfoProto> commitInfos = watchAllCommittedReply.getCommitInfos();
        final String message = "logIndex=" + logIndex + ", " + ProtoUtils.toString(commitInfos);
        Assert.assertEquals(NUM_SERVERS, commitInfos.size());
        commitInfos.forEach(info -> Assert.assertTrue(message, logIndex <= info.getCommitIndex()));
      }
    }
  }

  static <T> void assertNotDone(List<CompletableFuture<T>> futures) {
    assertNotDone(futures.stream());
  }

  static <T> void assertNotDone(Stream<CompletableFuture<T>> futures) {
    futures.forEach(f -> {
      if (f.isDone()) {
        try {
          Assert.fail("Done unexpectedly: " + f.get());
        } catch(Exception e) {
          Assert.fail("Done unexpectedly and failed to get: " + e);
        }
      }
    });
  }

  @Test
  public void testWatchRequestAsyncChangeLeader() throws Exception {
    runWithNewCluster(NUM_SERVERS,
        cluster -> runTest(WatchRequestTests::runTestWatchRequestAsyncChangeLeader, cluster, LOG));
  }

  static void runTestWatchRequestAsyncChangeLeader(TestParameters p) throws Exception {
    final Logger LOG = p.log;
    final MiniRaftCluster cluster = p.cluster;
    final int numMessages = p.numMessages;

    // blockFlushStateMachineData a follower so that no transaction can be ALL_COMMITTED
    final List<RaftServerImpl> followers = cluster.getFollowers();
    final RaftServerImpl blockedFollower = followers.get(ThreadLocalRandom.current().nextInt(followers.size()));
    LOG.info("block follower {}", blockedFollower.getId());
    SimpleStateMachine4Testing.get(blockedFollower).blockFlushStateMachineData();

    final List<CompletableFuture<RaftClientReply>> replies = new ArrayList<>();
    final List<CompletableFuture<WatchReplies>> watches = new ArrayList<>();

    p.sendRequests(replies, watches);

    Assert.assertEquals(numMessages, replies.size());
    Assert.assertEquals(numMessages, watches.size());

    // since only one follower is blocked, requests can be committed MAJORITY but neither ALL nor ALL_COMMITTED.
    checkMajority(replies, watches, LOG);

    TimeUnit.SECONDS.sleep(1);
    assertNotDone(watches.stream().map(CompletableFuture::join).map(w -> w.all));
    assertNotDone(watches.stream().map(CompletableFuture::join).map(w -> w.allCommitted));

    // Now change leader
    RaftTestUtil.changeLeader(cluster, cluster.getLeader().getId());

    // unblock follower so that the transaction can be replicated and committed to all.
    SimpleStateMachine4Testing.get(blockedFollower).unblockFlushStateMachineData();
    LOG.info("unblock follower {}", blockedFollower.getId());
    checkAll(watches, LOG);
  }

  @Test
  public void testWatchRequestTimeout() throws Exception {
    final RaftProperties p = getProperties();
    RaftServerConfigKeys.setWatchTimeout(p, TimeDuration.valueOf(500, TimeUnit.MILLISECONDS));
    RaftServerConfigKeys.setWatchTimeoutDenomination(p, TimeDuration.valueOf(100, TimeUnit.MILLISECONDS));
    try {
      runWithNewCluster(NUM_SERVERS,
          cluster -> runTest(WatchRequestTests::runTestWatchRequestTimeout, cluster, LOG));
    } finally {
      RaftServerConfigKeys.setWatchTimeout(p, RaftServerConfigKeys.WATCH_TIMEOUT_DEFAULT);
      RaftServerConfigKeys.setWatchTimeoutDenomination(p, RaftServerConfigKeys.WATCH_TIMEOUT_DENOMINATION_DEFAULT);
    }
  }

  static void runTestWatchRequestTimeout(TestParameters p) throws Exception {
    final Logger LOG = p.log;
    final MiniRaftCluster cluster = p.cluster;
    final int numMessages = p.numMessages;

    final TimeDuration watchTimeout = RaftServerConfigKeys.watchTimeout(cluster.getProperties());
    final TimeDuration watchTimeoutDenomination = RaftServerConfigKeys.watchTimeoutDenomination(cluster.getProperties());

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
    final List<CompletableFuture<WatchReplies>> watches = new ArrayList<>();

    p.sendRequests(replies, watches);

    Assert.assertEquals(numMessages, replies.size());
    Assert.assertEquals(numMessages, watches.size());

    watchTimeout.sleep();
    watchTimeoutDenomination.sleep(); // for roundup error
    assertNotDone(replies);
    assertNotDone(watches);

    // unblock leader so that the transaction can be committed.
    SimpleStateMachine4Testing.get(leader).unblockStartTransaction();
    LOG.info("unblock leader {}", leader.getId());

    checkMajority(replies, watches, LOG);
    checkTimeout(replies, watches, LOG);

    SimpleStateMachine4Testing.get(blockedFollower).unblockFlushStateMachineData();
    LOG.info("unblock follower {}", blockedFollower.getId());
  }

  static void checkTimeout(List<CompletableFuture<RaftClientReply>> replies,
      List<CompletableFuture<WatchReplies>> watches, Logger LOG) throws Exception {
    for(int i = 0; i < replies.size(); i++) {
      final RaftClientReply reply = replies.get(i).get(GET_TIMEOUT_SECOND, TimeUnit.SECONDS);
      LOG.info("checkTimeout {}: receive {}", i, reply);
      final long logIndex = reply.getLogIndex();
      Assert.assertTrue(reply.isSuccess());

      final WatchReplies watchReplies = watches.get(i).get(GET_TIMEOUT_SECOND, TimeUnit.SECONDS);
      Assert.assertEquals(logIndex, watchReplies.logIndex);

      final RaftClientReply watchAllReply = watchReplies.getAll();
      assertNotReplicatedException(logIndex, ReplicationLevel.ALL, watchAllReply);

      final RaftClientReply watchAllCommittedReply = watchReplies.getAllCommitted();
      assertNotReplicatedException(logIndex, ReplicationLevel.ALL_COMMITTED, watchAllCommittedReply);
    }
  }

  static void assertNotReplicatedException(long logIndex, ReplicationLevel replication, RaftClientReply reply) {
    Assert.assertFalse(reply.isSuccess());
    final NotReplicatedException nre = reply.getNotReplicatedException();
    Assert.assertNotNull(nre);
    Assert.assertEquals(logIndex, nre.getLogIndex());
    Assert.assertEquals(replication, nre.getRequiredReplication());
  }
}
