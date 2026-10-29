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

import org.apache.ratis.BaseTest;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.proto.RaftProtos.RequestVoteReplyProto;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.TransferLeadershipRequest;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.RaftServerRpc;
import org.apache.ratis.server.ServerFactory;
import org.apache.ratis.server.leader.FollowerInfo;
import org.apache.ratis.server.leader.LeaderState.StepDownReason;
import org.apache.ratis.server.leader.LogAppender;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.server.storage.RaftStorageMetadataFile;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.Timestamp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@Timeout(30)
public class TestLeaderStepDown extends BaseTest {
  private static final RaftPeerId LEADER = RaftPeerId.valueOf("s1");
  private static final RaftPeerId FOLLOWER = RaftPeerId.valueOf("s2");
  private static final RaftPeerId OTHER = RaftPeerId.valueOf("s3");

  @ParameterizedTest
  @CsvSource({"false, false, false", "true, false, false", "false, true, false", "true, true, false",
      "true, false, true", "true, true, true"})
  public void testHigherTermBypassesPendingStepDown(boolean processorStarted, boolean cleanupFails,
      boolean updateCommitFirst) throws Exception {
    final RaftGroup group = newGroup();
    final File storageVolume = new File(getTestDir(), "storage");
    FileUtils.deleteFully(storageVolume);
    final CompletableFuture<Void> stopped = new CompletableFuture<>();

    try (RaftServerImpl server = newServer(group, storageVolume, RaftStorage.StartupOption.FORMAT, stopped)) {
      server.start();
      final LeaderStateImpl leader = becomeLeader(server);
      final FollowerInfo follower = leader.getLogAppenders().findFirst().orElseThrow(AssertionError::new).getFollower();
      final Thread processor = (Thread) RaftTestUtil.getDeclaredField(leader, "processor");
      if (processorStarted) {
        leader.start();
        // prepare() also takes the server lock; let it finish before blocking event execution.
        RaftTestUtil.waitFor(() -> processor.getState() == Thread.State.TIMED_WAITING, 10, 5000);
      }

      try {
        final CompletableFuture<RaftClientReply> reply = CompletableFuture.supplyAsync(() -> {
          synchronized (server) {
            try {
              if (updateCommitFirst) {
                // A startup flush may put UPDATE_COMMIT ahead of the local step-down request.
                leader.submitUpdateCommitEvent();
                RaftTestUtil.waitFor(() -> isBlockedOnServer(processor, server), 10, 5000);
              }
              final CompletableFuture<RaftClientReply> pending = leader.submitStepDownRequestAsync(
                  new TransferLeadershipRequest(ClientId.randomId(), LEADER, group.getGroupId(), 1, null, 20000));
              if (processorStarted) {
                // The processor cannot execute its polled event while this callback holds the server monitor.
                // Other events, including STEP_DOWN, may still be queued behind that event.
                RaftTestUtil.waitFor(() -> isBlockedOnServer(processor, server), 10, 5000);
              }
              Assertions.assertEquals(1L, server.getState().getCurrentTerm());
              Assertions.assertTrue(server.getInfo().isLeader(), "FORCE must remain queued");
              Assertions.assertTrue(leader.onFollowerTerm(follower, 3));
              Assertions.assertEquals(3L, server.getState().getCurrentTerm(), "higher terms must bypass the queue");
              Assertions.assertEquals(3L, loadPersistedTerm(server));
              Assertions.assertTrue(server.getInfo().isFollower());
              final FollowerState followerState = server.getRole().getFollowerState().orElseThrow(AssertionError::new);
              Assertions.assertEquals(StepDownReason.HIGHER_TERM, RaftTestUtil.getDeclaredField(followerState, "reason"));
              Assertions.assertFalse(stopped.isDone());
              Assertions.assertFalse(pending.isDone(), "admin reply must wait for appender cleanup");
              assertStaleVoteRejected(server, group);
              return pending;
            } catch (Exception e) {
              throw new CompletionException(e);
            }
          }
        }).get(5, TimeUnit.SECONDS);
        // A callback can be running in an appender: it must return before that same appender has stopped.
        Assertions.assertFalse(stopped.isDone());
        final IOException failure = new IOException("Appender cleanup failed");
        if (cleanupFails) {
          stopped.completeExceptionally(failure);
          final ExecutionException thrown = Assertions.assertThrows(ExecutionException.class,
              () -> reply.get(5, TimeUnit.SECONDS));
          Throwable cause = thrown.getCause();
          while (cause.getCause() != null) {
            cause = cause.getCause();
          }
          Assertions.assertSame(failure, cause);
        } else {
          stopped.complete(null);
          Assertions.assertTrue(reply.get(5, TimeUnit.SECONDS).isSuccess());
        }
      } finally {
        // Release cleanup before closing the division, including after an assertion fails.
        stopped.complete(null);
        if (processorStarted) {
          processor.join(5000);
          Assertions.assertFalse(processor.isAlive(), "step-down event processor did not stop");
        }
      }
    }
  }

  @Test
  public void testHigherTermSurvivesCloseBeforeEventProcessing() throws Exception {
    final RaftGroup group = newGroup();
    final File storageVolume = new File(getTestDir(), "storage");
    FileUtils.deleteFully(storageVolume);
    try (RaftServerImpl server = newServer(group, storageVolume)) {
      server.start();
      final LeaderStateImpl leader = becomeLeader(server);
      final FollowerInfo follower = leader.getLogAppenders().findFirst().orElseThrow(AssertionError::new).getFollower();
      leader.submitStepDownEvent(StepDownReason.FORCE);
      synchronized (server) {
        Assertions.assertTrue(leader.onFollowerTerm(follower, 3));
      }
      final Thread processor = (Thread) RaftTestUtil.getDeclaredField(leader, "processor");
      Assertions.assertEquals(Thread.State.NEW, processor.getState());
      server.close();
    }
    try (RaftServerImpl restarted = newServer(group, storageVolume, RaftStorage.StartupOption.RECOVER,
        CompletableFuture.completedFuture(null))) {
      restarted.start();
      Assertions.assertEquals(3L, restarted.getState().getCurrentTerm());
      Assertions.assertEquals(3L, loadPersistedTerm(restarted));
      assertStaleVoteRejected(restarted, group);
    }
  }

  @Test
  public void testIgnoreUncaughtUpAndNonHigherTerms() throws Exception {
    final RaftGroup group = newGroup();
    final File storageVolume = new File(getTestDir(), "storage");
    FileUtils.deleteFully(storageVolume);
    try (RaftServerImpl server = newServer(group, storageVolume)) {
      server.start();
      final LeaderStateImpl leader = becomeLeader(server);
      final FollowerInfo follower = leader.getLogAppenders().findFirst().orElseThrow(AssertionError::new).getFollower();
      final FollowerInfo uncaughtUp = new FollowerInfoImpl(server.getMemberId(), peer(OTHER), group::getPeer,
          Timestamp.currentTime(), 0, false);
      synchronized (server) {
        Assertions.assertFalse(leader.onFollowerTerm(follower, 0));
        Assertions.assertFalse(leader.onFollowerTerm(follower, 1));
        Assertions.assertFalse(leader.onFollowerTerm(uncaughtUp, 3));
      }
      Assertions.assertTrue(server.getInfo().isLeader());
      Assertions.assertEquals(1L, server.getState().getCurrentTerm());
      Assertions.assertEquals(1L, loadPersistedTerm(server));
    }
  }

  @Test
  public void testHigherTermMetadataFailureStopsDivision() throws Exception {
    final RaftGroup group = newGroup();
    final File storageVolume = new File(getTestDir(), "storage");
    FileUtils.deleteFully(storageVolume);
    try (RaftServerImpl server = newServer(group, storageVolume)) {
      server.start();
      final LeaderStateImpl leader = becomeLeader(server);
      final FollowerInfo follower = leader.getLogAppenders().findFirst().orElseThrow(AssertionError::new).getFollower();
      final IOException failure = new IOException("Failed to persist higher follower term");
      Mockito.doThrow(failure).when(spyMetadataFile(server)).persist(Mockito.any());
      final CompletableFuture<RaftClientReply> pending = leader.submitStepDownRequestAsync(
          new TransferLeadershipRequest(ClientId.randomId(), LEADER, group.getGroupId(), 1, null, 20000));
      synchronized (server) {
        Assertions.assertTrue(leader.onFollowerTerm(follower, 3));
        Assertions.assertTrue(pending.isCompletedExceptionally());
        final ExecutionException thrown = Assertions.assertThrows(ExecutionException.class,
            () -> pending.get(5, TimeUnit.SECONDS));
        Assertions.assertSame(failure, thrown.getCause());
      }
      Assertions.assertFalse(server.getInfo().isAlive());
      final ExecutionException thrown = Assertions.assertThrows(ExecutionException.class,
          () -> closeFuture(server).get(5, TimeUnit.SECONDS));
      Assertions.assertSame(failure, thrown.getCause());
      Assertions.assertEquals(LifeCycle.State.CLOSED, server.getInfo().getLifeCycleState());
    }
  }

  private static LeaderStateImpl becomeLeader(RaftServerImpl server) throws IOException {
    synchronized (server) {
      server.getRole().shutdownFollowerState();
      server.getState().updateCurrentTerm(1);
      server.getState().persistMetadata();
      server.getRole().transitionRole(RaftPeerRole.LEADER);
      final LeaderStateImpl leader = server.getRole().updateLeaderState(server);
      server.getState().becomeLeader();
      return leader;
    }
  }

  private static void assertStaleVoteRejected(RaftServerImpl server, RaftGroup group) throws IOException {
    final RequestVoteReplyProto reply = server.requestVote(ServerProtoUtils.toRequestVoteRequestProto(
        RaftGroupMemberId.valueOf(FOLLOWER, group.getGroupId()), LEADER, 2,
        server.getState().getLog().getLastEntryTermIndex(), false));
    Assertions.assertFalse(reply.getServerReply().getSuccess());
    Assertions.assertEquals(3L, reply.getTerm());
  }

  private static long loadPersistedTerm(RaftServerImpl server) throws IOException {
    final Properties metadata = new Properties();
    final File metadataFile = new File(server.getRaftStorage().getStorageDir().getCurrentDir(), "raft-meta");
    try (InputStream in = Files.newInputStream(metadataFile.toPath())) {
      metadata.load(in);
    }
    return Long.parseLong(metadata.getProperty("term"));
  }

  @SuppressWarnings("unchecked")
  private static RaftStorageMetadataFile spyMetadataFile(RaftServerImpl server) {
    final RaftStorage storage = server.getRaftStorage();
    final RaftStorageMetadataFile metadata = Mockito.spy(storage.getMetadataFile());
    final Object metaFile = RaftTestUtil.getDeclaredField(storage, "metaFile");
    final AtomicReference<RaftStorageMetadataFile> ref =
        (AtomicReference<RaftStorageMetadataFile>) RaftTestUtil.getDeclaredField(metaFile, "ref");
    ref.set(metadata);
    return metadata;
  }

  @SuppressWarnings("unchecked")
  private static CompletableFuture<Boolean> closeFuture(RaftServerImpl server) {
    return (CompletableFuture<Boolean>) RaftTestUtil.getDeclaredField(server, "closeFuture");
  }

  private static RaftGroup newGroup() {
    return RaftGroup.valueOf(RaftGroupId.randomId(), Arrays.asList(peer(LEADER), peer(FOLLOWER), peer(OTHER)));
  }

  private static boolean isBlockedOnServer(Thread thread, RaftServerImpl server) {
    final ThreadInfo info = ManagementFactory.getThreadMXBean().getThreadInfo(thread.getId());
    return info != null && info.getThreadState() == Thread.State.BLOCKED && info.getLockInfo() != null
        && info.getLockInfo().getIdentityHashCode() == System.identityHashCode(server);
  }

  private static RaftPeer peer(RaftPeerId id) {
    return RaftPeer.newBuilder().setId(id).setAddress("127.0.0.1:0").build();
  }

  private static RaftServerImpl newServer(RaftGroup group, File storageVolume) throws Exception {
    return newServer(group, storageVolume, RaftStorage.StartupOption.FORMAT, CompletableFuture.completedFuture(null));
  }

  private static RaftServerImpl newServer(RaftGroup group, File storageVolume, RaftStorage.StartupOption option,
      CompletableFuture<Void> stopped) throws Exception {
    final RaftProperties properties = new RaftProperties();
    RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storageVolume));
    RaftServerConfigKeys.Rpc.setTimeoutMin(properties, TimeDuration.valueOf(60, TimeUnit.SECONDS));
    RaftServerConfigKeys.Rpc.setTimeoutMax(properties, TimeDuration.valueOf(61, TimeUnit.SECONDS));
    RaftServerConfigKeys.Rpc.setFirstElectionTimeoutMin(properties, TimeDuration.valueOf(60, TimeUnit.SECONDS));
    RaftServerConfigKeys.Rpc.setFirstElectionTimeoutMax(properties, TimeDuration.valueOf(61, TimeUnit.SECONDS));

    final ServerFactory factory = Mockito.mock(ServerFactory.class);
    Mockito.when(factory.newLogAppender(Mockito.any(), Mockito.any(), Mockito.any())).thenAnswer(invocation -> {
      final FollowerInfo follower = invocation.getArgument(2);
      final LogAppender appender = Mockito.mock(LogAppender.class);
      Mockito.when(appender.getFollower()).thenReturn(follower);
      Mockito.when(appender.getFollowerId()).thenReturn(follower.getId());
      Mockito.doReturn(stopped).when(appender).stopAsync();
      return appender;
    });
    final RaftServerProxy proxy = Mockito.mock(RaftServerProxy.class);
    Mockito.when(proxy.getId()).thenReturn(LEADER);
    Mockito.when(proxy.getPeer()).thenReturn(group.getPeer(LEADER));
    Mockito.when(proxy.getProperties()).thenReturn(properties);
    Mockito.when(proxy.getThreadGroup()).thenReturn(new ThreadGroup("leader-step-down"));
    Mockito.when(proxy.getServerRpc()).thenReturn(Mockito.mock(RaftServerRpc.class));
    Mockito.when(proxy.getFactory()).thenReturn(factory);
    return new RaftServerImpl(group, new BaseStateMachine(), proxy, option);
  }
}
