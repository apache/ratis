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
import org.apache.ratis.client.impl.RaftClientTestUtil;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.BlockRequestHandlingInjection;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.impl.RaftServerProxy;
import org.apache.ratis.server.impl.RaftServerTestUtil;
import org.apache.ratis.server.impl.RetryCacheTestUtil;
import org.apache.ratis.server.storage.RaftLog;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.util.ExitUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.LogUtils;
import org.apache.ratis.util.TimeDuration;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.ratis.RaftTestUtil.waitForLeader;

public abstract class RaftBasicTests<CLUSTER extends MiniRaftCluster>
    extends BaseTest
    implements MiniRaftCluster.Factory.Get<CLUSTER> {
  {
    LogUtils.setLogLevel(RaftServerImpl.LOG, Level.DEBUG);
    LogUtils.setLogLevel(RaftServerTestUtil.getStateMachineUpdaterLog(), Level.DEBUG);
    LogUtils.setLogLevel(RaftClient.LOG, Level.DEBUG);

    RaftServerConfigKeys.RetryCache.setExpiryTime(getProperties(), TimeDuration.valueOf(5, TimeUnit.SECONDS));
  }

  public static final int NUM_SERVERS = 5;

  @Test
  public void testBasicAppendEntries() throws Exception {
    runWithNewCluster(NUM_SERVERS, cluster ->
        runTestBasicAppendEntries(false, false, 10, cluster, LOG));
  }

  @Test
  public void testBasicAppendEntriesKillLeader() throws Exception {
    runWithNewCluster(NUM_SERVERS, cluster ->
        runTestBasicAppendEntries(false, true, 10, cluster, LOG));
  }

  static CompletableFuture<Void> killAndRestartServer(
      RaftPeerId id, long killSleepMs, long restartSleepMs, MiniRaftCluster cluster, Logger LOG) {
    final CompletableFuture<Void> future = new CompletableFuture<>();
    new Thread(() -> {
      try {
        Thread.sleep(killSleepMs);
        cluster.killServer(id);
        Thread.sleep(restartSleepMs);
        LOG.info("restart server: " + id);
        cluster.restartServer(id, false);
        future.complete(null);
      } catch (Exception e) {
        ExitUtils.terminate(-1, "Failed to kill/restart server: " + id, e, LOG);
      }
    }).start();
    return future;
  }

  static void runTestBasicAppendEntries(
      boolean async, boolean killLeader, int numMessages, MiniRaftCluster cluster, Logger LOG)
      throws Exception {
    LOG.info("runTestBasicAppendEntries: async? {}, killLeader={}, numMessages={}",
        async, killLeader, numMessages);
    for (RaftServer s : cluster.getServers()) {
      cluster.restartServer(s.getId(), false);
    }
    RaftServerImpl leader = waitForLeader(cluster);
    final long term = leader.getState().getCurrentTerm();

    final CompletableFuture<Void> killAndRestartFollower = killAndRestartServer(
        cluster.getFollowers().get(0).getId(), 0, 1000, cluster, LOG);
    final CompletableFuture<Void> killAndRestartLeader;
    if (killLeader) {
      LOG.info("killAndRestart leader " + leader.getId());
      killAndRestartLeader = killAndRestartServer(leader.getId(), 2000, 4000, cluster, LOG);
    } else {
      killAndRestartLeader = CompletableFuture.completedFuture(null);
    }

    LOG.info(cluster.printServers());

    final SimpleMessage[] messages = SimpleMessage.create(numMessages);

    try (final RaftClient client = cluster.createClient()) {
      final AtomicInteger asyncReplyCount = new AtomicInteger();
      final CompletableFuture<Void> f = new CompletableFuture<>();

      for (SimpleMessage message : messages) {
        if (async) {
          client.sendAsync(message).thenAcceptAsync(reply -> {
            if (!reply.isSuccess()) {
              f.completeExceptionally(
                  new AssertionError("Failed with reply " + reply));
            } else if (asyncReplyCount.incrementAndGet() == messages.length) {
              f.complete(null);
            }
          });
        } else {
          final RaftClientReply reply = client.send(message);
          Assert.assertTrue(reply.isSuccess());
        }
      }
      if (async) {
        f.join();
        Assert.assertEquals(messages.length, asyncReplyCount.get());
      }
    }
    Thread.sleep(cluster.getTimeoutMax().toInt(TimeUnit.MILLISECONDS) + 100);
    LOG.info(cluster.printAllLogs());
    killAndRestartFollower.join();
    killAndRestartLeader.join();

    for(RaftServerProxy server : cluster.getServers()) {
      final RaftServerImpl impl = RaftServerTestUtil.getRaftServerImpl(server, cluster.getGroupId());
      if (impl.isAlive()) {
        JavaUtils.attempt(() -> RaftTestUtil.assertLogEntries(impl, term, messages),
            5, TimeDuration.valueOf(1, TimeUnit.SECONDS), impl.getId() + " assertLogEntries", LOG);
      }
    }
  }


  @Test
  public void testOldLeaderCommit() throws Exception {
    runWithNewCluster(NUM_SERVERS, this::runTestOldLeaderCommit);
  }

  void runTestOldLeaderCommit(CLUSTER cluster) throws Exception {
    final RaftServerImpl leader = waitForLeader(cluster);
    final RaftPeerId leaderId = leader.getId();
    final long term = leader.getState().getCurrentTerm();

    List<RaftServerImpl> followers = cluster.getFollowers();
    final RaftServerImpl followerToSendLog = followers.get(0);
    for (int i = 1; i < NUM_SERVERS - 1; i++) {
      RaftServerImpl follower = followers.get(i);
      cluster.killServer(follower.getId());
    }

    SimpleMessage[] messages = SimpleMessage.create(1);
    RaftTestUtil.sendMessageInNewThread(cluster, messages);

    Thread.sleep(cluster.getMaxTimeout() + 100);
    RaftLog followerLog = followerToSendLog.getState().getLog();
    Assert.assertTrue(RaftTestUtil.logEntriesContains(followerLog, messages));

    LOG.info(String.format("killing old leader: %s", leaderId.toString()));
    cluster.killServer(leaderId);

    for (int i = 1; i < 3; i++) {
      RaftServerImpl follower = followers.get(i);
      LOG.info(String.format("restarting follower: %s", follower.getId().toString()));
      cluster.restartServer(follower.getId(), false );
    }

    Thread.sleep(cluster.getMaxTimeout() * 5);
    // confirm the server with log is elected as new leader.
    final RaftPeerId newLeaderId = waitForLeader(cluster).getId();
    Assert.assertEquals(followerToSendLog.getId(), newLeaderId);

    cluster.getServerAliveStream().map(s -> s.getState().getLog())
        .forEach(log -> RaftTestUtil.assertLogEntries(log, term, messages));
  }

  @Test
  public void testOldLeaderNotCommit() throws Exception {
    runWithNewCluster(NUM_SERVERS, this::runTestOldLeaderNotCommit);
  }

  void runTestOldLeaderNotCommit(CLUSTER cluster) throws Exception {
    final RaftPeerId leaderId = waitForLeader(cluster).getId();

    List<RaftServerImpl> followers = cluster.getFollowers();
    final RaftServerImpl followerToCommit = followers.get(0);
    for (int i = 1; i < NUM_SERVERS - 1; i++) {
      RaftServerImpl follower = followers.get(i);
      cluster.killServer(follower.getId());
    }

    SimpleMessage[] messages = SimpleMessage.create(1);
    RaftTestUtil.sendMessageInNewThread(cluster, messages);

    Thread.sleep(cluster.getMaxTimeout() + 100);
    RaftTestUtil.logEntriesContains(followerToCommit.getState().getLog(), messages);

    cluster.killServer(leaderId);
    cluster.killServer(followerToCommit.getId());

    for (int i = 1; i < NUM_SERVERS - 1; i++) {
      RaftServerImpl follower = followers.get(i);
      cluster.restartServer(follower.getId(), false );
    }
    waitForLeader(cluster);
    Thread.sleep(cluster.getMaxTimeout() + 100);

    final Predicate<LogEntryProto> predicate = l -> l.getTerm() != 1;
    cluster.getServerAliveStream()
            .map(s -> s.getState().getLog())
            .forEach(log -> RaftTestUtil.checkLogEntries(log, messages, predicate));
  }

  static class Client4TestWithLoad extends Thread {
    boolean useAsync;
    final int index;
    final SimpleMessage[] messages;

    final AtomicBoolean isRunning = new AtomicBoolean(true);
    final AtomicInteger step = new AtomicInteger();
    final AtomicReference<Throwable> exceptionInClientThread = new AtomicReference<>();

    final MiniRaftCluster cluster;
    final Logger LOG;

    Client4TestWithLoad(int index, int numMessages, boolean useAsync,
        MiniRaftCluster cluster, Logger LOG) {
      super("client-" + index);
      this.index = index;
      this.messages = SimpleMessage.create(numMessages, index + "-");
      this.useAsync = useAsync;
      this.cluster = cluster;
      this.LOG = LOG;
    }

    boolean isRunning() {
      return isRunning.get();
    }

    @Override
    public void run() {
      try (RaftClient client = cluster.createClient()) {
        final CompletableFuture<Void> f = new CompletableFuture<>();
        for (int i = 0; i < messages.length; i++) {
          if (!useAsync) {
            final RaftClientReply reply =
                client.send(messages[step.getAndIncrement()]);
            Assert.assertTrue(reply.isSuccess());
          } else {
            final CompletableFuture<RaftClientReply> replyFuture =
                client.sendAsync(messages[i]);
            replyFuture.thenAcceptAsync(r -> {
              if (!r.isSuccess()) {
                f.completeExceptionally(
                    new AssertionError("Failed with reply: " + r));
              }
              if (step.incrementAndGet() == messages.length) {
                f.complete(null);
              }
              Assert.assertTrue(r.isSuccess());
            });
          }
        }
        if (useAsync) {
          f.join();
          Assert.assertTrue(step.get() == messages.length);
        }
      } catch(Throwable t) {
        if (exceptionInClientThread.compareAndSet(null, t)) {
          LOG.error(this + " failed", t);
        } else {
          exceptionInClientThread.get().addSuppressed(t);
          LOG.error(this + " failed again!", t);
        }
      } finally {
        isRunning.set(false);
      }
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + index
          + "(step=" + step + "/" + messages.length
          + ", isRunning=" + isRunning
          + ", isAlive=" + isAlive()
          + ", exception=" + exceptionInClientThread
          + ")";
    }
  }

  @Test
  public void testWithLoad() throws Exception {
    runWithNewCluster(NUM_SERVERS, cluster -> testWithLoad(10, 300, false, cluster, LOG));
  }

  static void testWithLoad(final int numClients, final int numMessages,
      boolean useAsync, MiniRaftCluster cluster, Logger LOG) throws Exception {
    LOG.info("Running testWithLoad: numClients=" + numClients
        + ", numMessages=" + numMessages + ", async=" + useAsync);

    waitForLeader(cluster);

    final List<Client4TestWithLoad> clients
        = Stream.iterate(0, i -> i+1).limit(numClients)
        .map(i -> new Client4TestWithLoad(i, numMessages, useAsync, cluster, LOG))
        .collect(Collectors.toList());
    final AtomicInteger lastStep = new AtomicInteger();

    final Timer timer = new Timer();
    timer.schedule(new TimerTask() {
      private int previousLastStep = lastStep.get();

      @Override
      public void run() {
        LOG.info(cluster.printServers());
        LOG.info(BlockRequestHandlingInjection.getInstance().toString());
        LOG.info(cluster.toString());
        clients.forEach(c -> LOG.info("  " + c));
        JavaUtils.dumpAllThreads(s -> LOG.info(s));

        final int last = lastStep.get();
        if (last != previousLastStep) {
          previousLastStep = last;
        } else {
          final RaftServerImpl leader = cluster.getLeader();
          LOG.info("NO PROGRESS at " + last + ", try to restart leader=" + leader);
          if (leader != null) {
            try {
              cluster.restartServer(leader.getId(), false);
              LOG.info("Restarted leader=" + leader);
            } catch (IOException e) {
              LOG.error("Failed to restart leader=" + leader);
            }
          }
        }
      }
    }, 5_000L, 10_000L);

    clients.forEach(Thread::start);

    int count = 0;
    for(;; ) {
      if (clients.stream().noneMatch(Client4TestWithLoad::isRunning)) {
        break;
      }

      final int n = clients.stream().mapToInt(c -> c.step.get()).sum();
      Assert.assertTrue(n >= lastStep.get());

      if (n - lastStep.get() < 50 * numClients) { // Change leader at least 50 steps.
        Thread.sleep(10);
        continue;
      }
      lastStep.set(n);
      count++;

      try {
        RaftServerImpl leader = cluster.getLeader();
        if (leader != null) {
          RaftTestUtil.changeLeader(cluster, leader.getId());
        }
      } catch (IllegalStateException e) {
        LOG.error("Failed to change leader ", e);
      }
    }
    LOG.info("Leader change count=" + count);
    timer.cancel();

    for(Client4TestWithLoad c : clients) {
      if (c.exceptionInClientThread.get() != null) {
        throw new AssertionError(c.exceptionInClientThread.get());
      }
      RaftTestUtil.assertLogEntries(cluster, c.messages);
    }
  }

  public static void testRequestTimeout(boolean async, MiniRaftCluster cluster, Logger LOG) throws Exception {
    waitForLeader(cluster);
    long time = System.currentTimeMillis();
    try (final RaftClient client = cluster.createClient()) {
      // Get the next callId to be used by the client
      long callId = RaftClientTestUtil.getCallId(client);
      // Create an entry corresponding to the callId and clientId
      // in each server's retry cache.
      cluster.getServerAliveStream().forEach(
          raftServer -> RetryCacheTestUtil.getOrCreateEntry(raftServer.getRetryCache(), client.getId(), callId));
      // Client request for the callId now waits
      // as there is already a cache entry in the server for the request.
      // Ideally the client request should timeout and the client should retry.
      // The retry is successful when the retry cache entry for the corresponding callId and clientId expires.
      if (async) {
        CompletableFuture<RaftClientReply> replyFuture = client.sendAsync(new SimpleMessage("abc"));
        replyFuture.get();
      } else {
        client.send(new SimpleMessage("abc"));
      }
      // Eventually the request would be accepted by the server
      // when the retry cache entry is invalidated.
      // The duration for which the client waits should be more than the retryCacheExpiryDuration.
      TimeDuration duration = TimeDuration.valueOf(System.currentTimeMillis() - time, TimeUnit.MILLISECONDS);
      TimeDuration retryCacheExpiryDuration = RaftServerConfigKeys.RetryCache.expiryTime(cluster.getProperties());
      Assert.assertTrue(duration.compareTo(retryCacheExpiryDuration) >= 0);
    }
  }
}
