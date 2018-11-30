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
import org.apache.ratis.client.impl.RaftClientTestUtil;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.*;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.proto.RaftProtos.CommitInfoProto;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.statemachine.SimpleStateMachine4Testing;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.LogUtils;
import org.apache.ratis.util.TimeDuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.ratis.RaftTestUtil.waitForLeader;

public abstract class RaftAsyncTests<CLUSTER extends MiniRaftCluster> extends BaseTest
    implements MiniRaftCluster.Factory.Get<CLUSTER> {
  static {
    LogUtils.setLogLevel(RaftServerImpl.LOG, Level.DEBUG);
    LogUtils.setLogLevel(RaftClient.LOG, Level.DEBUG);
  }

  public static final int NUM_SERVERS = 3;

  @Before
  public void setup() {
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

  @Test
  public void testRequestAsyncWithRetryPolicy() throws Exception {
    LOG.info("Running testWatchRequestsWithRetryPolicy");
    try(final CLUSTER cluster = newCluster(NUM_SERVERS)) {
     int maxRetries = 3;
      final RetryPolicy retryPolicy = RetryPolicies
          .retryUpToMaximumCountWithFixedSleep(maxRetries, TimeDuration.valueOf(1, TimeUnit.SECONDS));
      cluster.start();
      final RaftClient writeClient =
          cluster.createClient(RaftTestUtil.waitForLeader(cluster).getId(), retryPolicy);
      // blockStartTransaction of the leader so that no transaction can be committed MAJORITY
      final RaftServerImpl leader = cluster.getLeader();
      LOG.info("block leader {}", leader.getId());
      SimpleStateMachine4Testing.get(leader).blockStartTransaction();
      RaftClientReply reply =
          writeClient.sendAsync(RaftTestUtil.SimpleMessage.create(1)[0]).get();
      RaftRetryFailureException rfe = reply.getRetryFailureException();
      Assert.assertTrue(rfe != null);
      Assert.assertTrue(rfe.getMessage().contains(retryPolicy.toString()));
      // unblock leader so that the next transaction can be committed.
      SimpleStateMachine4Testing.get(leader).unblockStartTransaction();
      // make sure the the next request succeeds. This will ensure the first
      // request completed
      writeClient.sendAsync(RaftTestUtil.SimpleMessage.create(1)[0]).get();
      }
    }

  @Test
  public void testAsyncRequestSemaphore() throws Exception {
    LOG.info("Running testAsyncRequestSemaphore");
    final CLUSTER cluster = newCluster(NUM_SERVERS);
    Assert.assertNull(cluster.getLeader());
    cluster.start();
    waitForLeader(cluster);

    int numMessages = RaftClientConfigKeys.Async.maxOutstandingRequests(getProperties());
    CompletableFuture[] futures = new CompletableFuture[numMessages + 1];
    final RaftTestUtil.SimpleMessage[] messages = RaftTestUtil.SimpleMessage.create(numMessages);
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
    Assert.assertTrue(blockedRequestsCount.get() == 0);

    futures[numMessages] = CompletableFuture.supplyAsync(() -> {
      blockedRequestsCount.incrementAndGet();
      client.sendAsync(new RaftTestUtil.SimpleMessage("n1"));
      blockedRequestsCount.decrementAndGet();
      return null;
    });

    //Allow the last msg to be sent
    while (blockedRequestsCount.get() != 1) {
      Thread.sleep(1000);
    }
    Assert.assertTrue(blockedRequestsCount.get() == 1);
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
    Assert.assertTrue(blockedRequestsCount.get() == 0);
    cluster.shutdown();
  }

  void runTestBasicAppendEntriesAsync(boolean killLeader) throws Exception {
    final CLUSTER cluster = newCluster(killLeader? 5: 3);
    try {
      cluster.start();
      waitForLeader(cluster);
      RaftBasicTests.runTestBasicAppendEntries(true, killLeader, 100, cluster, LOG);
    } finally {
      cluster.shutdown();
    }
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
    LOG.info("Running testWithLoadAsync");
    final CLUSTER cluster = newCluster(NUM_SERVERS);
    cluster.start();
    waitForLeader(cluster);
    RaftBasicTests.testWithLoad(10, 500, true, cluster, LOG);
    cluster.shutdown();
  }

  @Test
  public void testStaleReadAsync() throws Exception {
    final int numMesssages = 10;
    final CLUSTER cluster = newCluster(NUM_SERVERS);

    try (RaftClient client = cluster.createClient()) {
      cluster.start();
      RaftTestUtil.waitForLeader(cluster);

      // submit some messages
      final List<CompletableFuture<RaftClientReply>> futures = new ArrayList<>();
      for (int i = 0; i < numMesssages; i++) {
        final String s = "" + i;
        LOG.info("sendAsync " + s);
        futures.add(client.sendAsync(new RaftTestUtil.SimpleMessage(s)));
      }
      Assert.assertEquals(numMesssages, futures.size());
      RaftClientReply lastWriteReply = null;
      for (CompletableFuture<RaftClientReply> f : futures) {
        lastWriteReply = f.join();
        Assert.assertTrue(lastWriteReply.isSuccess());
      }
      futures.clear();

      // Use a follower with the max commit index
      final RaftPeerId leader = lastWriteReply.getServerId();
      LOG.info("leader = " + leader);
      final Collection<CommitInfoProto> commitInfos = lastWriteReply.getCommitInfos();
      LOG.info("commitInfos = " + commitInfos);
      final CommitInfoProto followerCommitInfo = commitInfos.stream()
          .filter(info -> !RaftPeerId.valueOf(info.getServer().getId()).equals(leader))
          .max(Comparator.comparing(CommitInfoProto::getCommitIndex)).get();
      final RaftPeerId follower = RaftPeerId.valueOf(followerCommitInfo.getServer().getId());
      LOG.info("max follower = " + follower);

      // test a failure case
      testFailureCaseAsync("sendStaleReadAsync(..) with a larger commit index",
          () -> client.sendStaleReadAsync(
              new RaftTestUtil.SimpleMessage("" + Long.MAX_VALUE),
              followerCommitInfo.getCommitIndex(), follower),
          StateMachineException.class, IndexOutOfBoundsException.class);

      // test sendStaleReadAsync
      for (int i = 0; i < numMesssages; i++) {
        final int query = i;
        LOG.info("sendStaleReadAsync, query=" + query);
        final Message message = new RaftTestUtil.SimpleMessage("" + query);
        final CompletableFuture<RaftClientReply> readFuture = client.sendReadOnlyAsync(message);
        final CompletableFuture<RaftClientReply> staleReadFuture = client.sendStaleReadAsync(
            message, followerCommitInfo.getCommitIndex(), follower);

        futures.add(readFuture.thenApply(r -> getMessageContent(r))
            .thenCombine(staleReadFuture.thenApply(r -> getMessageContent(r)), (expected, computed) -> {
              try {
                LOG.info("query " + query + " returns "
                    + LogEntryProto.parseFrom(expected).getStateMachineLogEntry().getLogData().toStringUtf8());
              } catch (InvalidProtocolBufferException e) {
                throw new CompletionException(e);
              }

              Assert.assertEquals("log entry mismatch for query=" + query, expected, computed);
              return null;
            })
        );
      }
      JavaUtils.allOf(futures).join();
    } finally {
      cluster.shutdown();
    }
  }

  static ByteString getMessageContent(RaftClientReply reply) {
    Assert.assertTrue(reply.isSuccess());
    return reply.getMessage().getContent();
  }

  @Test
  public void testRequestTimeout() throws Exception {
    final TimeDuration oldExpiryTime = RaftServerConfigKeys.RetryCache.expiryTime(getProperties());
    RaftServerConfigKeys.RetryCache.setExpiryTime(getProperties(), TimeDuration.valueOf(5, TimeUnit.SECONDS));
    final CLUSTER cluster = newCluster(NUM_SERVERS);
    cluster.start();
    RaftBasicTests.testRequestTimeout(true, cluster, LOG);
    cluster.shutdown();

    //reset for the other tests
    RaftServerConfigKeys.RetryCache.setExpiryTime(getProperties(), oldExpiryTime);
  }

  @Test
  public void testAppendEntriesTimeout()
      throws IOException, InterruptedException, ExecutionException {
    LOG.info("Running testAppendEntriesTimeout");
    final TimeDuration oldExpiryTime = RaftServerConfigKeys.RetryCache.expiryTime(getProperties());
    RaftServerConfigKeys.RetryCache.setExpiryTime(getProperties(), TimeDuration.valueOf(20, TimeUnit.SECONDS));
    final CLUSTER cluster = newCluster(NUM_SERVERS);
    cluster.start();
    waitForLeader(cluster);
    long time = System.currentTimeMillis();
    long waitTime = 5000;
    try (final RaftClient client = cluster.createClient()) {
      // block append requests
      cluster.getServerAliveStream()
          .filter(impl -> !impl.isLeader())
          .map(SimpleStateMachine4Testing::get)
          .forEach(SimpleStateMachine4Testing::blockWriteStateMachineData);

      CompletableFuture<RaftClientReply> replyFuture = client.sendAsync(new RaftTestUtil.SimpleMessage("abc"));
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
    cluster.shutdown();

    //reset for the other tests
    RaftServerConfigKeys.RetryCache.setExpiryTime(getProperties(), oldExpiryTime);
  }
}
