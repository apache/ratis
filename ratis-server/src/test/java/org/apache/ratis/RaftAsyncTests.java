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
import org.apache.ratis.client.impl.RaftClientTestUtil;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.impl.RaftServerProxy;
import org.apache.ratis.shaded.com.google.protobuf.ByteString;
import org.apache.ratis.shaded.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.shaded.proto.RaftProtos.CommitInfoProto;
import org.apache.ratis.shaded.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.statemachine.SimpleStateMachine4Testing;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.LogUtils;
import org.apache.ratis.util.TimeDuration;
import org.junit.*;

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

  private RaftProperties properties;

  public static final int NUM_SERVERS = 3;

  @Before
  public void setup() {
    properties = new RaftProperties();
    properties.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        SimpleStateMachine4Testing.class, StateMachine.class);
    TimeDuration retryCacheExpiryDuration = TimeDuration.valueOf(5, TimeUnit.SECONDS);
    RaftServerConfigKeys.RetryCache.setExpiryTime(properties, retryCacheExpiryDuration);
  }

  @Test
  public void testAsyncConfiguration() throws IOException {
    LOG.info("Running testAsyncConfiguration");
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

    // reset to default for other tests.
    RaftClientConfigKeys.Async.setMaxOutstandingRequests(properties,
        RaftClientConfigKeys.Async.MAX_OUTSTANDING_REQUESTS_DEFAULT);
    RaftClientConfigKeys.Async.setSchedulerThreads(properties,
        RaftClientConfigKeys.Async.SCHEDULER_THREADS_DEFAULT);
  }

  @Test
  public void testAsyncRequestSemaphore()
      throws InterruptedException, IOException {
    LOG.info("Running testAsyncRequestSemaphore");
    CLUSTER cluster = getFactory().newCluster(NUM_SERVERS, properties);
    Assert.assertNull(cluster.getLeader());
    cluster.start();
    waitForLeader(cluster);

    int numMessages = RaftClientConfigKeys.Async.maxOutstandingRequests(properties);
    CompletableFuture[] futures = new CompletableFuture[numMessages + 1];
    final RaftTestUtil.SimpleMessage[] messages = RaftTestUtil.SimpleMessage.create(numMessages);
    final RaftClient client = cluster.createClient();
    //Set blockTransaction flag so that transaction blocks
    for (RaftServerProxy server : cluster.getServers()) {
      ((SimpleStateMachine4Testing) server.getStateMachine()).setBlockTransaction(true);
    }

    //Send numMessages which are blocked and do not release the client semaphore permits
    AtomicInteger blockedRequestsCount = new AtomicInteger();
    for (int i=0; i<numMessages; i++) {
      blockedRequestsCount.getAndIncrement();
      futures[i] = client.sendAsync(messages[i]);
      blockedRequestsCount.decrementAndGet();
    }
    Assert.assertTrue(blockedRequestsCount.get() == 0);

    ExecutorService threadPool = Executors.newFixedThreadPool(1);
    futures[numMessages] = CompletableFuture.supplyAsync(() -> {
      blockedRequestsCount.incrementAndGet();
      client.sendAsync(new RaftTestUtil.SimpleMessage("n1"));
      blockedRequestsCount.decrementAndGet();
      return null;
    }, threadPool);

    //Allow the last msg to be sent
    while (blockedRequestsCount.get() != 1) {
      Thread.sleep(1000);
    }
    Assert.assertTrue(blockedRequestsCount.get() == 1);
    //Since all semaphore permits are acquired the last message sent is in queue
    RaftClientTestUtil.assertAsyncRequestSemaphore(client, 0, 1);

    //Unset the blockTransaction flag so that semaphore permits can be released
    for (RaftServerProxy server : cluster.getServers()) {
      ((SimpleStateMachine4Testing) server.getStateMachine()).setBlockTransaction(false);
    }
    for(int i=0; i<=numMessages; i++){
      futures[i].join();
    }
    Assert.assertTrue(blockedRequestsCount.get() == 0);
    cluster.shutdown();
  }

  @Test
  public void testBasicAppendEntriesAsync() throws Exception {
    LOG.info("Running testBasicAppendEntriesAsync");
    RaftClientConfigKeys.Async.setMaxOutstandingRequests(properties, 100);
    final CLUSTER cluster = getFactory().newCluster(NUM_SERVERS, properties);
    cluster.start();
    waitForLeader(cluster);
    RaftBasicTests.runTestBasicAppendEntries(true, 1000, cluster, LOG);
    cluster.shutdown();
  }

  @Test
  public void testWithLoadAsync() throws Exception {
    LOG.info("Running testWithLoadAsync");
    RaftClientConfigKeys.Async.setMaxOutstandingRequests(properties, 100);
    final CLUSTER cluster = getFactory().newCluster(NUM_SERVERS, properties);
    cluster.start();
    waitForLeader(cluster);
    RaftBasicTests.testWithLoad(10, 500, true, cluster, LOG);
    cluster.shutdown();
  }

  @Test
  public void testStaleReadAsync() throws Exception {
    final int numMesssages = 10;
    final CLUSTER cluster = getFactory().newCluster(NUM_SERVERS, properties);

    try (RaftClient client = cluster.createClient()) {
      cluster.start();
      RaftTestUtil.waitForLeader(cluster);

      // submit some messages
      final List<CompletableFuture<RaftClientReply>> futures = new ArrayList<>();
      for (int i = 0; i < numMesssages; i++) {
        final String s = "m" + i;
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
              new RaftTestUtil.SimpleMessage("" + (numMesssages + 1)),
              followerCommitInfo.getCommitIndex(), follower),
          StateMachineException.class, IndexOutOfBoundsException.class);

      // test sendStaleReadAsync
      for (int i = 1; i < followerCommitInfo.getCommitIndex(); i++) {
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
                    + LogEntryProto.parseFrom(expected).getSmLogEntry().getData().toStringUtf8());
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
  public void testRequestTimeout()
      throws IOException, InterruptedException, ExecutionException {
    final CLUSTER cluster = getFactory().newCluster(NUM_SERVERS, properties);
    cluster.start();
    RaftBasicTests.testRequestTimeout(true, cluster, LOG, properties);
    cluster.shutdown();
  }

  @Test
  public void testAppendEntriesTimeout()
      throws IOException, InterruptedException, ExecutionException {
    LOG.info("Running testAppendEntriesTimeout");
    TimeDuration retryCacheExpiryDuration = TimeDuration.valueOf(20, TimeUnit.SECONDS);
    RaftServerConfigKeys.RetryCache.setExpiryTime(properties, retryCacheExpiryDuration);
    final CLUSTER cluster = getFactory().newCluster(NUM_SERVERS, properties);
    cluster.start();
    waitForLeader(cluster);
    long time = System.currentTimeMillis();
    long waitTime = 5000;
    try (final RaftClient client = cluster.createClient()) {
      // block append requests
      cluster.getServerAliveStream().forEach(raftServer -> {
        try {
          if (!raftServer.isLeader()) {
            ((SimpleStateMachine4Testing) raftServer.getStateMachine()).setBlockAppend(true);
          }
        } catch (InterruptedException e) {
          LOG.error("Interrupted while blocking append", e);
        }
      });
      CompletableFuture<RaftClientReply> replyFuture = client.sendAsync(new RaftTestUtil.SimpleMessage("abc"));
      Thread.sleep(waitTime);
      // replyFuture should not be completed until append request is unblocked.
      Assert.assertTrue(!replyFuture.isDone());
      // unblock append request.
      cluster.getServerAliveStream().forEach(raftServer -> {
        try {
          ((SimpleStateMachine4Testing) raftServer.getStateMachine()).setBlockAppend(false);
        } catch (InterruptedException e) {
          LOG.error("Interrupted while unblocking append", e);
        }
      });
      client.send(new RaftTestUtil.SimpleMessage("abc"));
      replyFuture.get();
      Assert.assertTrue(System.currentTimeMillis() - time > waitTime);
    }
    cluster.shutdown();
  }
}
