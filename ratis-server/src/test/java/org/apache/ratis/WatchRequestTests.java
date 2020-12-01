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
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.CommitInfoProto;
import org.apache.ratis.proto.RaftProtos.ReplicationLevel;
import org.apache.ratis.protocol.exceptions.AlreadyClosedException;
import org.apache.ratis.protocol.exceptions.NotReplicatedException;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.exceptions.RaftRetryFailureException;
import org.apache.ratis.protocol.exceptions.TimeoutIOException;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.server.impl.RaftServerTestUtil;
import org.apache.ratis.statemachine.SimpleStateMachine4Testing;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.Log4jUtils;
import org.apache.ratis.util.ProtoUtils;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.function.CheckedConsumer;
import org.apache.ratis.util.function.CheckedSupplier;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.Assert.fail;

public abstract class WatchRequestTests<CLUSTER extends MiniRaftCluster>
    extends BaseTest
    implements MiniRaftCluster.Factory.Get<CLUSTER> {
  {
    RaftServerTestUtil.setWatchRequestsLogLevel(Level.DEBUG);
    Log4jUtils.setLogLevel(RaftServer.Division.LOG, Level.DEBUG);
  }

  static final int NUM_SERVERS = 3;
  static final int GET_TIMEOUT_SECOND = 10;

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
    final MiniRaftCluster cluster;
    final Logger log;

    TestParameters(int numMessages, RaftClient writeClient,
        MiniRaftCluster cluster, Logger log) {
      this.numMessages = numMessages;
      this.writeClient = writeClient;
      this.cluster = cluster;
      this.log = log;
    }

    void sendRequests(List<CompletableFuture<RaftClientReply>> replies,
        List<CompletableFuture<WatchReplies>> watches) {
      for(int i = 0; i < numMessages; i++) {
        final String message = "m" + i;
        log.info("SEND_REQUEST {}: message={}", i, message);
        final CompletableFuture<RaftClientReply> replyFuture =
            writeClient.async().send(new RaftTestUtil.SimpleMessage(message));
        replies.add(replyFuture);
        final CompletableFuture<WatchReplies> watchFuture = new CompletableFuture<>();
        watches.add(watchFuture);
        replyFuture.thenAccept(reply -> {
          final long logIndex = reply.getLogIndex();
          log.info("SEND_WATCH: message={}, logIndex={}", message, logIndex);
          watchFuture.complete(new WatchReplies(logIndex,
              writeClient.async().watch(logIndex, ReplicationLevel.MAJORITY),
              writeClient.async().watch(logIndex, ReplicationLevel.ALL),
              writeClient.async().watch(logIndex, ReplicationLevel.MAJORITY_COMMITTED),
              writeClient.async().watch(logIndex, ReplicationLevel.ALL_COMMITTED),
              log));
        });
      }
    }

    CompletableFuture<RaftClientReply> sendWatchRequest(long logIndex, RetryPolicy policy)
        throws Exception {

      try (final RaftClient watchClient =
          cluster.createClient(RaftTestUtil.waitForLeader(cluster).getId(), policy)) {

        CompletableFuture<RaftClientReply> reply =
            watchClient.async().send(new RaftTestUtil.SimpleMessage("message"));
        long writeIndex = reply.get().getLogIndex();
        Assert.assertTrue(writeIndex > 0);
        watchClient.async().watch(writeIndex, ReplicationLevel.MAJORITY_COMMITTED);
        return watchClient.async().watch(logIndex, ReplicationLevel.MAJORITY);
      }

    }

    @Override
    public String toString() {
      return "numMessages=" + numMessages;
    }
  }

  static void runTest(CheckedConsumer<TestParameters, Exception> testCase, MiniRaftCluster cluster, Logger LOG)
      throws Exception {
    try(final RaftClient client = cluster.createClient(RaftTestUtil.waitForLeader(cluster).getId())) {
      final int[] numMessages = {1, 10, 20};
      for(int n : numMessages) {
        final TestParameters p = new TestParameters(n, client, cluster, LOG);
        LOG.info("{}) {}, {}", n, p, cluster.printServers());
        testCase.accept(p);
      }
    }
  }

  static void runSingleTest(CheckedConsumer<TestParameters, Exception> testCase,
      MiniRaftCluster cluster, Logger LOG)
      throws Exception {
    try(final RaftClient client = cluster.createClient(RaftTestUtil.waitForLeader(cluster).getId())) {
      final int[] numMessages = {1};
      for(int n : numMessages) {
        final TestParameters p = new TestParameters(n, client, cluster, LOG);
        LOG.info("{}) {}, {}", n, p, cluster.printServers());
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
      return get(majority, "majority");
    }

    RaftClientReply getMajorityCommitted() throws Exception {
      return get(majorityCommitted, "majorityCommitted");
    }

    RaftClientReply getAll() throws Exception {
      return get(all, "all");
    }

    RaftClientReply getAllCommitted() throws Exception {
      return get(allCommitted, "allCommitted");
    }

    RaftClientReply get(CompletableFuture<RaftClientReply> f, String name) throws Exception {
      final RaftClientReply reply;
      try {
        reply = f.get(GET_TIMEOUT_SECOND, TimeUnit.SECONDS);
      } catch (Exception e) {
        log.error("Failed to get {}({})", name, logIndex);
        throw e;
      }
      log.info("{}-Watch({}) returns {}", name, logIndex, reply);
      return reply;
    }
  }

  static void runTestWatchRequestAsync(TestParameters p) throws Exception {
    final Logger LOG = p.log;
    final MiniRaftCluster cluster = p.cluster;
    final int numMessages = p.numMessages;

    // blockStartTransaction of the leader so that no transaction can be committed MAJORITY
    final RaftServer.Division leader = cluster.getLeader();
    LOG.info("block leader {}", leader.getId());
    SimpleStateMachine4Testing.get(leader).blockStartTransaction();

    // blockFlushStateMachineData a follower so that no transaction can be ALL_COMMITTED
    final List<RaftServer.Division> followers = cluster.getFollowers();
    final RaftServer.Division blockedFollower = followers.get(ThreadLocalRandom.current().nextInt(followers.size()));
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
          fail("Done unexpectedly: " + f.get());
        } catch(Exception e) {
          fail("Done unexpectedly and failed to get: " + e);
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
    final List<RaftServer.Division> followers = cluster.getFollowers();
    final RaftServer.Division blockedFollower = followers.get(ThreadLocalRandom.current().nextInt(followers.size()));
    LOG.info("block follower {}", blockedFollower.getId());
    SimpleStateMachine4Testing.get(blockedFollower).blockFlushStateMachineData();

    final List<CompletableFuture<RaftClientReply>> replies = new ArrayList<>();
    final List<CompletableFuture<WatchReplies>> watches = new ArrayList<>();

    p.sendRequests(replies, watches);

    Assert.assertEquals(numMessages, replies.size());
    Assert.assertEquals(numMessages, watches.size());

    // since only one follower is blocked commit, requests can be committed MAJORITY and ALL but not ALL_COMMITTED.
    checkMajority(replies, watches, LOG);

    TimeUnit.SECONDS.sleep(1);
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
    RaftServerConfigKeys.Watch.setTimeout(p, TimeDuration.valueOf(500, TimeUnit.MILLISECONDS));
    RaftServerConfigKeys.Watch.setTimeoutDenomination(p, TimeDuration.valueOf(100, TimeUnit.MILLISECONDS));
    try {
      runWithNewCluster(NUM_SERVERS,
          cluster -> runTest(WatchRequestTests::runTestWatchRequestTimeout, cluster, LOG));
    } finally {
      RaftServerConfigKeys.Watch.setTimeout(p, RaftServerConfigKeys.Watch.TIMEOUT_DEFAULT);
      RaftServerConfigKeys.Watch.setTimeoutDenomination(p, RaftServerConfigKeys.Watch.TIMEOUT_DENOMINATION_DEFAULT);
    }
  }

  static void runTestWatchRequestTimeout(TestParameters p) throws Exception {
    final Logger LOG = p.log;
    final MiniRaftCluster cluster = p.cluster;
    final int numMessages = p.numMessages;

    final RaftProperties properties = cluster.getProperties();
    final TimeDuration watchTimeout = RaftServerConfigKeys.Watch.timeout(properties);
    final TimeDuration watchTimeoutDenomination = RaftServerConfigKeys.Watch.timeoutDenomination(properties);

    // blockStartTransaction of the leader so that no transaction can be committed MAJORITY
    final RaftServer.Division leader = cluster.getLeader();
    LOG.info("block leader {}", leader.getId());
    SimpleStateMachine4Testing.get(leader).blockStartTransaction();

    // blockFlushStateMachineData a follower so that no transaction can be ALL_COMMITTED
    final List<RaftServer.Division> followers = cluster.getFollowers();
    final RaftServer.Division blockedFollower = followers.get(ThreadLocalRandom.current().nextInt(followers.size()));
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

  @Test
  public void testWatchRequestClientTimeout() throws Exception {
    final RaftProperties p = getProperties();
    RaftServerConfigKeys.Watch.setTimeout(p, TimeDuration.valueOf(100,
        TimeUnit.SECONDS));
    RaftClientConfigKeys.Rpc.setWatchRequestTimeout(p,
        TimeDuration.valueOf(15, TimeUnit.SECONDS));
    try {
      runWithNewCluster(NUM_SERVERS,
          cluster -> runSingleTest(WatchRequestTests::runTestWatchRequestClientTimeout, cluster, LOG));
    } finally {
      RaftServerConfigKeys.Watch.setTimeout(p, RaftServerConfigKeys.Watch.TIMEOUT_DEFAULT);
      RaftClientConfigKeys.Rpc.setWatchRequestTimeout(p,
          RaftClientConfigKeys.Rpc.WATCH_REQUEST_TIMEOUT_DEFAULT);
    }
  }

  static void runTestWatchRequestClientTimeout(TestParameters p) throws Exception {
    final Logger LOG = p.log;

    CompletableFuture<RaftClientReply> watchReply;
    // watch 1000 which will never be committed
    // so client can not receive reply, and connection closed, throw TimeoutException.
    // We should not retry, because if retry, RaftClientImpl::handleIOException will random select a leader,
    // then sometimes throw NotLeaderException.
    watchReply = p.sendWatchRequest(1000, RetryPolicies.noRetry());

    try {
      watchReply.get();
      fail("runTestWatchRequestClientTimeout failed");
    } catch (Exception ex) {
      LOG.error("error occurred", ex);
      Assert.assertTrue(ex.getCause().getClass() == AlreadyClosedException.class ||
          ex.getCause().getClass() == RaftRetryFailureException.class);
      if (ex.getCause() != null) {
        if (ex.getCause().getCause() != null) {
          Assert.assertEquals(TimeoutIOException.class,
              ex.getCause().getCause().getClass());
        }
      }
    }
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

      assertNotReplicatedException(logIndex, ReplicationLevel.ALL, watchReplies::getAll);

      assertNotReplicatedException(logIndex, ReplicationLevel.ALL_COMMITTED, watchReplies::getAllCommitted);
    }
  }

  static void assertNotReplicatedException(long logIndex, ReplicationLevel replication,
      CheckedSupplier<RaftClientReply, Exception> replySupplier) throws Exception {
    try {
      replySupplier.get();
      fail();
    } catch (ExecutionException e) {
      final Throwable cause = e.getCause();
      assertNotReplicatedException(logIndex, replication, cause);
    }
  }

  static void assertNotReplicatedException(long logIndex, ReplicationLevel replication, Throwable t) {
    Assert.assertSame(NotReplicatedException.class, t.getClass());
    final NotReplicatedException nre = (NotReplicatedException) t;
    Assert.assertNotNull(nre);
    Assert.assertEquals(logIndex, nre.getLogIndex());
    Assert.assertEquals(replication, nre.getRequiredReplication());
  }
}
