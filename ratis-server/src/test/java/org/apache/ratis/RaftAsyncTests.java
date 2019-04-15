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
import org.apache.ratis.RaftTestUtil.SimpleMessage;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.client.impl.RaftClientTestUtil;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.CommitInfoProto;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.protocol.AlreadyClosedException;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.RaftRetryFailureException;
import org.apache.ratis.protocol.StateMachineException;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.retry.RetryPolicies.RetryLimited;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.statemachine.SimpleStateMachine4Testing;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.LogUtils;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.function.CheckedRunnable;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.ratis.RaftTestUtil.waitForLeader;

public abstract class RaftAsyncTests<CLUSTER extends MiniRaftCluster> extends BaseTest
    implements MiniRaftCluster.Factory.Get<CLUSTER> {
  {
    LogUtils.setLogLevel(RaftServerImpl.LOG, Level.DEBUG);
    LogUtils.setLogLevel(RaftClient.LOG, Level.DEBUG);
  }

  public static final int NUM_SERVERS = 3;

  {
    getProperties().setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        SimpleStateMachine4Testing.class, StateMachine.class);
  }

  @Test
  public void testAsyncConfiguration() throws IOException {
    LOG.info("Running testAsyncConfiguration");
    final RaftProperties properties = new RaftProperties();
    RaftClient.Builder clientBuilder = RaftClient.newBuilder()
        .setRaftGroup(RaftGroup.emptyGroup())
        .setProperties(properties);
    int numThreads = RaftClientConfigKeys.Async.SCHEDULER_THREADS_DEFAULT;
    int maxOutstandingRequests = RaftClientConfigKeys.Async.MAX_OUTSTANDING_REQUESTS_DEFAULT;
    try(RaftClient client = clientBuilder.build()) {
      RaftClientTestUtil.assertScheduler(client, numThreads);
      RaftClientTestUtil.assertAsyncRequestSemaphore(client, maxOutstandingRequests, 0);
    }

    numThreads = 200;
    maxOutstandingRequests = 5;
    RaftClientConfigKeys.Async.setMaxOutstandingRequests(properties, maxOutstandingRequests);
    RaftClientConfigKeys.Async.setSchedulerThreads(properties, numThreads);
    try(RaftClient client = clientBuilder.build()) {
      RaftClientTestUtil.assertScheduler(client, numThreads);
      RaftClientTestUtil.assertAsyncRequestSemaphore(client, maxOutstandingRequests, 0);
    }
  }

  static void assertRaftRetryFailureException(RaftRetryFailureException rfe, RetryPolicy retryPolicy, String name) {
    Assert.assertNotNull(name + " does not have RaftRetryFailureException", rfe);
    Assert.assertTrue(name + ": unexpected error message, rfe=" + rfe + ", retryPolicy=" + retryPolicy,
        rfe.getMessage().contains(retryPolicy.toString()));
  }

  @Test
  public void testRequestAsyncWithRetryFailure() throws Exception {
    runWithNewCluster(1, false, cluster -> runTestRequestAsyncWithRetryFailure(false, cluster));
  }

  @Test
  public void testRequestAsyncWithRetryFailureAfterInitialMessages() throws Exception {
    runWithNewCluster(1, true, cluster -> runTestRequestAsyncWithRetryFailure(true, cluster));
  }

  void runTestRequestAsyncWithRetryFailure(boolean initialMessages, CLUSTER cluster) throws Exception {
    final RetryLimited retryPolicy = RetryPolicies.retryUpToMaximumCountWithFixedSleep(10, HUNDRED_MILLIS);

    try(final RaftClient client = cluster.createClient(null, retryPolicy)) {
      RaftPeerId leader = null;
      if (initialMessages) {
        // cluster is already started, send a few success messages
        leader = RaftTestUtil.waitForLeader(cluster).getId();
        final SimpleMessage[] messages = SimpleMessage.create(10, "initial-");
        final List<CompletableFuture<RaftClientReply>> replies = new ArrayList<>();
        for (int i = 0; i < messages.length; i++) {
          replies.add(client.sendAsync(messages[i]));
        }
        for (int i = 0; i < messages.length; i++) {
          RaftTestUtil.assertSuccessReply(replies.get(i));
        }

        // kill the only server
        cluster.killServer(leader);
      }

      // now, either the cluster is not yet started or the server is killed.
      final List<CompletableFuture<RaftClientReply>> replies = new ArrayList<>();
      {
        final SimpleMessage[] messages = SimpleMessage.create(10);
        int i = 0;
        // send half of the calls without starting the cluster
        for (; i < messages.length/2; i++) {
          replies.add(client.sendAsync(messages[i]));
        }

        // sleep most of the retry time
        retryPolicy.getSleepTime().apply(t -> t * (retryPolicy.getMaxAttempts() - 1)).sleep();

        // send another half of the calls without starting the cluster
        for (; i < messages.length; i++) {
          replies.add(client.sendAsync(messages[i]));
        }
        Assert.assertEquals(messages.length, replies.size());
      }

      // sleep again so that the first half calls will fail retries.
      // the second half still have retry time remaining.
      retryPolicy.getSleepTime().apply(t -> t*2).sleep();

      if (leader != null) {
        cluster.restartServer(leader, false);
      } else {
        cluster.start();
      }

      // all the calls should fail for ordering guarantee
      for(int i = 0; i < replies.size(); i++) {
        final CheckedRunnable<Exception> getReply = replies.get(i)::get;
        final String name = "retry-failure-" + i;
        if (i == 0) {
          final Throwable t = testFailureCase(name, getReply,
              ExecutionException.class, RaftRetryFailureException.class);
          assertRaftRetryFailureException((RaftRetryFailureException) t.getCause(), retryPolicy, name);
        } else {
          testFailureCase(name, getReply,
              ExecutionException.class, AlreadyClosedException.class, RaftRetryFailureException.class);
        }
      }

      testFailureCaseAsync("last-request", () -> client.sendAsync(new SimpleMessage("last")),
          AlreadyClosedException.class, RaftRetryFailureException.class);
    }
  }

  @Test
  public void testAsyncRequestSemaphore() throws Exception {
    runWithNewCluster(NUM_SERVERS, this::runTestAsyncRequestSemaphore);
  }

  void runTestAsyncRequestSemaphore(CLUSTER cluster) throws Exception {
    waitForLeader(cluster);

    int numMessages = RaftClientConfigKeys.Async.maxOutstandingRequests(getProperties());
    CompletableFuture[] futures = new CompletableFuture[numMessages + 1];
    final SimpleMessage[] messages = SimpleMessage.create(numMessages);
    final RaftClient client = cluster.createClient();
    //Set blockTransaction flag so that transaction blocks
    cluster.getServers().stream()
        .map(cluster::getRaftServerImpl)
        .map(SimpleStateMachine4Testing::get)
        .forEach(SimpleStateMachine4Testing::blockStartTransaction);

    //Send numMessages which are blocked and do not release the client semaphore permits
    AtomicInteger blockedRequestsCount = new AtomicInteger();
    for (int i=0; i<numMessages; i++) {
      blockedRequestsCount.getAndIncrement();
      futures[i] = client.sendAsync(messages[i]);
      blockedRequestsCount.decrementAndGet();
    }
    Assert.assertEquals(0, blockedRequestsCount.get());

    futures[numMessages] = CompletableFuture.supplyAsync(() -> {
      blockedRequestsCount.incrementAndGet();
      client.sendAsync(new SimpleMessage("n1"));
      blockedRequestsCount.decrementAndGet();
      return null;
    });

    //Allow the last msg to be sent
    while (blockedRequestsCount.get() != 1) {
      Thread.sleep(1000);
    }
    Assert.assertEquals(1, blockedRequestsCount.get());
    //Since all semaphore permits are acquired the last message sent is in queue
    RaftClientTestUtil.assertAsyncRequestSemaphore(client, 0, 1);

    //Unset the blockTransaction flag so that semaphore permits can be released
    cluster.getServers().stream()
        .map(cluster::getRaftServerImpl)
        .map(SimpleStateMachine4Testing::get)
        .forEach(SimpleStateMachine4Testing::unblockStartTransaction);

    for(int i=0; i<=numMessages; i++){
      futures[i].join();
    }
    Assert.assertEquals(0, blockedRequestsCount.get());
  }

  void runTestBasicAppendEntriesAsync(boolean killLeader) throws Exception {
    runWithNewCluster(killLeader? 5: 3,
        cluster -> RaftBasicTests.runTestBasicAppendEntries(true, killLeader, 100, cluster, LOG));
  }

  @Test
  public void testBasicAppendEntriesAsync() throws Exception {
    runTestBasicAppendEntriesAsync(false);
  }

  @Test
  public void testBasicAppendEntriesAsyncKillLeader() throws Exception {
    runTestBasicAppendEntriesAsync(true);
  }

  @Test
  public void testWithLoadAsync() throws Exception {
    runWithNewCluster(NUM_SERVERS,
        cluster -> RaftBasicTests.testWithLoad(10, 500, true, cluster, LOG));
  }

  @Test
  public void testStaleReadAsync() throws Exception {
    runWithNewCluster(NUM_SERVERS, this::runTestStaleReadAsync);
  }

  void runTestStaleReadAsync(CLUSTER cluster) throws Exception {
    final int numMesssages = 10;
    try (RaftClient client = cluster.createClient()) {
      RaftTestUtil.waitForLeader(cluster);

      // submit some messages
      final List<CompletableFuture<RaftClientReply>> futures = new ArrayList<>();
      for (int i = 0; i < numMesssages; i++) {
        final String s = "" + i;
        LOG.info("sendAsync " + s);
        futures.add(client.sendAsync(new SimpleMessage(s)));
      }
      Assert.assertEquals(numMesssages, futures.size());
      final List<RaftClientReply> replies = new ArrayList<>();
      for (CompletableFuture<RaftClientReply> f : futures) {
        final RaftClientReply r = f.join();
        Assert.assertTrue(r.isSuccess());
        replies.add(r);
      }
      futures.clear();

      // Use a follower with the max commit index
      final RaftClientReply lastWriteReply = replies.get(replies.size() - 1);
      final RaftPeerId leader = lastWriteReply.getServerId();
      LOG.info("leader = " + leader);
      final Collection<CommitInfoProto> commitInfos = lastWriteReply.getCommitInfos();
      LOG.info("commitInfos = " + commitInfos);
      final CommitInfoProto followerCommitInfo = commitInfos.stream()
          .filter(info -> !RaftPeerId.valueOf(info.getServer().getId()).equals(leader))
          .max(Comparator.comparing(CommitInfoProto::getCommitIndex)).get();
      final RaftPeerId follower = RaftPeerId.valueOf(followerCommitInfo.getServer().getId());
      final long followerCommitIndex = followerCommitInfo.getCommitIndex();
      LOG.info("max follower = {}, commitIndex = {}", follower, followerCommitIndex);

      // test a failure case
      testFailureCaseAsync("sendStaleReadAsync(..) with a larger commit index",
          () -> client.sendStaleReadAsync(
              new SimpleMessage("" + Long.MAX_VALUE),
              followerCommitInfo.getCommitIndex(), follower),
          StateMachineException.class, IndexOutOfBoundsException.class);

      // test sendStaleReadAsync
      for (int i = 0; i < numMesssages; i++) {
        final RaftClientReply reply = replies.get(i);
        final String query = "" + i;
        LOG.info("query=" + query + ", reply=" + reply);
        final Message message = new SimpleMessage(query);
        final CompletableFuture<RaftClientReply> readFuture = client.sendReadOnlyAsync(message);

        futures.add(readFuture.thenCompose(r -> {
          if (reply.getLogIndex() <= followerCommitIndex) {
            LOG.info("sendStaleReadAsync, query=" + query);
            return client.sendStaleReadAsync(message, followerCommitIndex, follower);
          } else {
            return CompletableFuture.completedFuture(null);
          }
        }).thenApply(staleReadReply -> {
          if (staleReadReply == null) {
            return null;
          }

          final ByteString expected = readFuture.join().getMessage().getContent();
          final ByteString computed = staleReadReply.getMessage().getContent();
          try {
            LOG.info("query " + query + " returns "
                + LogEntryProto.parseFrom(expected).getStateMachineLogEntry().getLogData().toStringUtf8());
          } catch (InvalidProtocolBufferException e) {
            throw new CompletionException(e);
          }

          Assert.assertEquals("log entry mismatch for query=" + query, expected, computed);
          return null;
        }));
      }
      JavaUtils.allOf(futures).join();
    }
  }

  @Test
  public void testRequestTimeout() throws Exception {
    final TimeDuration oldExpiryTime = RaftServerConfigKeys.RetryCache.expiryTime(getProperties());
    RaftServerConfigKeys.RetryCache.setExpiryTime(getProperties(), TimeDuration.valueOf(5, TimeUnit.SECONDS));
    runWithNewCluster(NUM_SERVERS, cluster -> RaftBasicTests.testRequestTimeout(true, cluster, LOG));

    //reset for the other tests
    RaftServerConfigKeys.RetryCache.setExpiryTime(getProperties(), oldExpiryTime);
  }

  @Test
  public void testAppendEntriesTimeout() throws Exception {
    runWithNewCluster(NUM_SERVERS, this::runTestAppendEntriesTimeout);
  }

  void runTestAppendEntriesTimeout(CLUSTER cluster) throws Exception {
    LOG.info("Running testAppendEntriesTimeout");
    final TimeDuration oldExpiryTime = RaftServerConfigKeys.RetryCache.expiryTime(getProperties());
    RaftServerConfigKeys.RetryCache.setExpiryTime(getProperties(), TimeDuration.valueOf(20, TimeUnit.SECONDS));
    waitForLeader(cluster);
    long time = System.currentTimeMillis();
    long waitTime = 5000;
    try (final RaftClient client = cluster.createClient()) {
      // block append requests
      cluster.getServerAliveStream()
          .filter(impl -> !impl.isLeader())
          .map(SimpleStateMachine4Testing::get)
          .forEach(SimpleStateMachine4Testing::blockWriteStateMachineData);

      CompletableFuture<RaftClientReply> replyFuture = client.sendAsync(new SimpleMessage("abc"));
      Thread.sleep(waitTime);
      // replyFuture should not be completed until append request is unblocked.
      Assert.assertTrue(!replyFuture.isDone());
      // unblock append request.
      cluster.getServerAliveStream()
          .filter(impl -> !impl.isLeader())
          .map(SimpleStateMachine4Testing::get)
          .forEach(SimpleStateMachine4Testing::unblockWriteStateMachineData);

      replyFuture.get();
      Assert.assertTrue(System.currentTimeMillis() - time > waitTime);
    }

    //reset for the other tests
    RaftServerConfigKeys.RetryCache.setExpiryTime(getProperties(), oldExpiryTime);
  }
}
