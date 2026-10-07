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
import org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto.AppendResult;
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotRequestProto;
import org.apache.ratis.proto.RaftProtos.RaftGroupIdProto;
import org.apache.ratis.proto.RaftProtos.RaftRpcRequestProto;
import org.apache.ratis.proto.RaftProtos.RequestVoteRequestProto;
import org.apache.ratis.proto.RaftProtos.RoleInfoProto;
import org.apache.ratis.proto.RaftProtos.StartLeaderElectionRequestProto;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.GroupMismatchException;
import org.apache.ratis.protocol.exceptions.ServerNotReadyException;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.RaftServerRpc;
import org.apache.ratis.server.protocol.RaftServerProtocol.Op;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.server.storage.RaftStorageMetadataFile;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.util.AtomicFileOutputStream;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.TimeDuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class TestRaftServerMetadataPersistence extends BaseTest {
  private static final RaftPeerId LEADER = RaftPeerId.valueOf("s1");
  private static final RaftPeerId FOLLOWER = RaftPeerId.valueOf("s2");
  private static final RaftPeerId OTHER = RaftPeerId.valueOf("s3");

  @ParameterizedTest
  @EnumSource(Op.class)
  public void testMetadataFailureStopsDivision(Op op) throws Exception {
    runTestMetadataFailure(op, false, false);
  }

  @Test
  public void testRejectedVoteMetadataFailureStopsDivision() throws Exception {
    runTestMetadataFailure(Op.REQUEST_VOTE, true, false);
  }

  @Test
  public void testSnapshotChunkMetadataFailureStopsDivision() throws Exception {
    runTestMetadataFailure(Op.INSTALL_SNAPSHOT, false, true);
  }

  @Test
  public void testSkipMetadataPersistenceForSameTermAppend() throws Exception {
    final RaftGroup group = RaftGroup.valueOf(RaftGroupId.randomId(),
        Arrays.asList(peer(LEADER), peer(FOLLOWER), peer(OTHER)));
    final File storageVolume = new File(getTestDir(), "storage");
    FileUtils.deleteFully(storageVolume);

    try (RaftServerImpl follower = newServer(group, storageVolume, RaftStorage.StartupOption.FORMAT)) {
      follower.start();
      Assertions.assertEquals(AppendResult.SUCCESS, follower.appendEntries(appendEntries(group, 1, 0)).getResult());
      final RaftStorageMetadataFile metadata = spyMetadataFile(follower);

      for (int i = 1; i <= 3; i++) {
        Assertions.assertEquals(AppendResult.SUCCESS, follower.appendEntries(appendEntries(group, 1, i)).getResult());
      }
      Mockito.verify(metadata, Mockito.never()).persist(Mockito.any());
      Assertions.assertEquals(1L, loadPersistedTerm(follower));
    }
  }

  @Test
  public void testElectionMetadataFailureStopsDivision() throws Exception {
    final RaftGroup group = RaftGroup.valueOf(RaftGroupId.randomId(),
        Arrays.asList(peer(LEADER), peer(FOLLOWER), peer(OTHER)));
    final File storageVolume = new File(getTestDir(), "storage");
    FileUtils.deleteFully(storageVolume);
    final ShutdownStateMachine stateMachine = new ShutdownStateMachine();

    try (RaftServerImpl follower = newServer(group, storageVolume, RaftStorage.StartupOption.FORMAT,
        stateMachine, false)) {
      follower.start();
      Assertions.assertEquals(AppendResult.SUCCESS, follower.appendEntries(appendEntries(group, 0, 0)).getResult());
      final IOException failure = new IOException("Failed to persist election metadata");
      Mockito.doThrow(failure).when(spyMetadataFile(follower)).persist(Mockito.any());

      final StartLeaderElectionRequestProto request = StartLeaderElectionRequestProto.newBuilder()
          .setServerRequest(appendEntries(group, 0, 1).getServerRequest())
          .setLeaderLastEntry(TermIndex.PROTO_DEFAULT.toProto())
          .build();
      final IllegalStateException thrown = Assertions.assertThrows(IllegalStateException.class,
          () -> follower.startLeaderElection(request));
      Assertions.assertSame(failure, thrown.getCause());
      assertStopped(follower, group, stateMachine);
    }
  }

  @Test
  public void testOriginalMetadataExceptionIsPreserved() throws Exception {
    final RaftGroup group = RaftGroup.valueOf(RaftGroupId.randomId(),
        Arrays.asList(peer(LEADER), peer(FOLLOWER), peer(OTHER)));
    final File storageVolume = new File(getTestDir(), "storage");
    FileUtils.deleteFully(storageVolume);
    final ShutdownStateMachine stateMachine = new ShutdownStateMachine();

    try (RaftServerImpl follower = newServer(group, storageVolume, RaftStorage.StartupOption.FORMAT,
        stateMachine, false)) {
      follower.start();
      final IOException failure = new IOException("Failed to persist metadata");
      Mockito.doThrow(failure).when(spyMetadataFile(follower)).persist(Mockito.any());

      final IOException thrown = Assertions.assertThrows(IOException.class,
          () -> follower.appendEntries(appendEntries(group, 1, 0)));
      Assertions.assertSame(failure, thrown);
      assertStopped(follower, group, stateMachine);
    }
  }

  @Test
  public void testMetadataFailureFromServerExecutorDoesNotDeadlock() throws Exception {
    final RaftGroup group = RaftGroup.valueOf(RaftGroupId.randomId(),
        Arrays.asList(peer(LEADER), peer(FOLLOWER), peer(OTHER)));
    final File storageVolume = new File(getTestDir(), "storage");
    FileUtils.deleteFully(storageVolume);
    final ShutdownStateMachine stateMachine = new ShutdownStateMachine();

    try (RaftServerImpl follower = newServer(group, storageVolume, RaftStorage.StartupOption.FORMAT,
        stateMachine, false)) {
      follower.start();
      final IOException failure = new IOException("Failed to persist metadata");
      Mockito.doThrow(failure).when(spyMetadataFile(follower)).persist(Mockito.any());

      final CompletableFuture<IOException> reply = CompletableFuture.supplyAsync(
          () -> Assertions.assertThrows(IOException.class, () -> follower.appendEntries(appendEntries(group, 1, 0))),
          follower.getServerExecutor());
      Assertions.assertSame(failure, reply.get(5, TimeUnit.SECONDS));
      assertStopped(follower, group, stateMachine);
    }
  }

  @Test
  public void testMetadataFailureStopsOnlyAffectedDivision() throws Exception {
    final RaftGroup group = RaftGroup.valueOf(RaftGroupId.randomId(),
        Arrays.asList(peer(LEADER), peer(FOLLOWER), peer(OTHER)));
    final RaftGroup otherGroup = RaftGroup.valueOf(RaftGroupId.randomId(), group.getPeers());
    final File storageVolume = new File(getTestDir(), "storage");
    FileUtils.deleteFully(storageVolume);
    final ShutdownStateMachine stateMachine = new ShutdownStateMachine();

    try (RaftServerImpl follower = newServer(group, storageVolume, RaftStorage.StartupOption.FORMAT,
        stateMachine, false);
         RaftServerImpl other = new RaftServerImpl(otherGroup, new BaseStateMachine(),
             (RaftServerProxy) follower.getRaftServer(), RaftStorage.StartupOption.FORMAT)) {
      follower.start();
      other.start();
      Mockito.doThrow(new IOException("Failed to persist metadata"))
          .when(spyMetadataFile(follower)).persist(Mockito.any());

      Assertions.assertThrows(IOException.class, () -> follower.appendEntries(appendEntries(group, 1, 0)));
      assertStopped(follower, group, stateMachine);
      Assertions.assertTrue(other.getInfo().isAlive());
      Assertions.assertEquals(AppendResult.SUCCESS, other.appendEntries(appendEntries(otherGroup, 1, 0)).getResult());
      Mockito.verify(follower.getRaftServer(), Mockito.never()).close();
    }
  }

  @Test
  public void testConcurrentCloseAfterMetadataFailureDoesNotDuplicateShutdown() throws Exception {
    final RaftGroup group = RaftGroup.valueOf(RaftGroupId.randomId(),
        Arrays.asList(peer(LEADER), peer(FOLLOWER), peer(OTHER)));
    final File storageVolume = new File(getTestDir(), "storage");
    FileUtils.deleteFully(storageVolume);
    final CountDownLatch closeStarted = new CountDownLatch(1);
    final CountDownLatch releaseClose = new CountDownLatch(1);
    final AtomicInteger closeCount = new AtomicInteger();
    final ShutdownStateMachine stateMachine = new ShutdownStateMachine() {
      @Override
      public void close() throws IOException {
        closeCount.incrementAndGet();
        closeStarted.countDown();
        try {
          Assertions.assertTrue(releaseClose.await(10, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException(e);
        }
        super.close();
      }
    };

    try (RaftServerImpl follower = newServer(group, storageVolume, RaftStorage.StartupOption.FORMAT,
        stateMachine, false)) {
      follower.start();
      final IOException failure = new IOException("Failed to persist metadata");
      Mockito.doThrow(failure).when(spyMetadataFile(follower)).persist(Mockito.any());
      Assertions.assertSame(failure, Assertions.assertThrows(IOException.class,
          () -> follower.appendEntries(appendEntries(group, 1, 0))));
      Assertions.assertTrue(closeStarted.await(5, TimeUnit.SECONDS));

      follower.close();
      Assertions.assertEquals(1, closeCount.get());
      Assertions.assertFalse(stateMachine.shutdown.isDone());
      releaseClose.countDown();
      assertStopped(follower, group, stateMachine);
      Assertions.assertEquals(1, closeCount.get());
    } finally {
      releaseClose.countDown();
    }
  }

  @Test
  public void testMetadataFailureNotifiesAfterConcurrentClose() throws Exception {
    final RaftGroup group = RaftGroup.valueOf(RaftGroupId.randomId(),
        Arrays.asList(peer(LEADER), peer(FOLLOWER), peer(OTHER)));
    final File storageVolume = new File(getTestDir(), "storage");
    FileUtils.deleteFully(storageVolume);
    final CountDownLatch persistenceStarted = new CountDownLatch(1);
    final CountDownLatch releasePersistence = new CountDownLatch(1);
    final CountDownLatch closeStarted = new CountDownLatch(1);
    final CountDownLatch releaseClose = new CountDownLatch(1);
    final ShutdownStateMachine stateMachine = new ShutdownStateMachine() {
      @Override
      public void close() throws IOException {
        closeStarted.countDown();
        try {
          Assertions.assertTrue(releaseClose.await(10, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException(e);
        }
        super.close();
      }
    };
    final ExecutorService executor = Executors.newFixedThreadPool(2);

    try (RaftServerImpl follower = newServer(group, storageVolume, RaftStorage.StartupOption.FORMAT,
        stateMachine, false)) {
      follower.start();
      final IOException failure = new IOException("Failed to persist metadata");
      Mockito.doAnswer(invocation -> {
        persistenceStarted.countDown();
        Assertions.assertTrue(releasePersistence.await(10, TimeUnit.SECONDS));
        throw failure;
      }).when(spyMetadataFile(follower)).persist(Mockito.any());

      final CompletableFuture<IOException> reply = CompletableFuture.supplyAsync(
          () -> Assertions.assertThrows(IOException.class, () -> follower.appendEntries(appendEntries(group, 1, 0))),
          executor);
      Assertions.assertTrue(persistenceStarted.await(5, TimeUnit.SECONDS));
      final CompletableFuture<Void> close = CompletableFuture.runAsync(follower::close, executor);
      Assertions.assertTrue(closeStarted.await(5, TimeUnit.SECONDS));
      Assertions.assertEquals(LifeCycle.State.CLOSING, follower.getInfo().getLifeCycleState());
      releasePersistence.countDown();

      Assertions.assertSame(failure, reply.get(5, TimeUnit.SECONDS));
      Assertions.assertFalse(stateMachine.shutdown.isDone());
      releaseClose.countDown();
      close.get(5, TimeUnit.SECONDS);
      assertStopped(follower, group, stateMachine);
    } finally {
      releasePersistence.countDown();
      releaseClose.countDown();
      executor.shutdownNow();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testQueuedSnapshotRejectedAfterMetadataFailure(boolean snapshotChunk) throws Exception {
    final RaftGroup group = RaftGroup.valueOf(RaftGroupId.randomId(),
        Arrays.asList(peer(LEADER), peer(FOLLOWER), peer(OTHER)));
    final File storageVolume = new File(getTestDir(), "storage");
    FileUtils.deleteFully(storageVolume);
    final ShutdownStateMachine stateMachine = new ShutdownStateMachine();

    try (RaftServerImpl follower = newServer(group, storageVolume, RaftStorage.StartupOption.FORMAT,
        stateMachine, snapshotChunk)) {
      follower.start();
      final IOException failure = new IOException("Failed to persist metadata");
      Mockito.doThrow(failure).when(spyMetadataFile(follower)).persist(Mockito.any());
      final CompletableFuture<Void> reply = new CompletableFuture<>();
      final Thread snapshot = new Thread(() -> {
        try {
          follower.installSnapshot(installSnapshot(group, snapshotChunk));
          reply.complete(null);
        } catch (Throwable e) {
          reply.completeExceptionally(e);
        }
      }, "queued-snapshot");
      snapshot.setDaemon(true);

      synchronized (follower) {
        snapshot.start();
        // Wait until the snapshot passes its outer lifecycle check and blocks on the server lock.
        RaftTestUtil.waitFor(() -> {
          final ThreadInfo info = ManagementFactory.getThreadMXBean().getThreadInfo(snapshot.getId());
          return info != null && info.getThreadState() == Thread.State.BLOCKED && info.getLockInfo() != null
              && info.getLockInfo().getIdentityHashCode() == System.identityHashCode(follower);
        }, 10, 5000);
        Assertions.assertSame(failure, Assertions.assertThrows(IOException.class,
            () -> follower.requestVote(requestVote(group, LEADER, 1))));
      }

      final ExecutionException thrown = Assertions.assertThrows(ExecutionException.class,
          () -> reply.get(5, TimeUnit.SECONDS));
      Assertions.assertInstanceOf(ServerNotReadyException.class, thrown.getCause());
      snapshot.join(5000);
      Assertions.assertFalse(snapshot.isAlive());
      assertStopped(follower, group, stateMachine);
    }
  }

  @Test
  public void testShutdownCallbackFailureDoesNotReplaceMetadataException() throws Exception {
    final RaftGroup group = RaftGroup.valueOf(RaftGroupId.randomId(),
        Arrays.asList(peer(LEADER), peer(FOLLOWER), peer(OTHER)));
    final File storageVolume = new File(getTestDir(), "storage");
    FileUtils.deleteFully(storageVolume);
    final AtomicReference<Thread> callbackThread = new AtomicReference<>();
    final AtomicReference<Throwable> uncaught = new AtomicReference<>();
    final ShutdownStateMachine stateMachine = new ShutdownStateMachine() {
      @Override
      public void notifyServerShutdown(RoleInfoProto roleInfo, boolean allServer) {
        callbackThread.set(Thread.currentThread());
        Thread.currentThread().setUncaughtExceptionHandler((thread, cause) -> uncaught.set(cause));
        super.notifyServerShutdown(roleInfo, allServer);
        throw new IllegalStateException("Failed shutdown callback");
      }
    };

    try (RaftServerImpl follower = newServer(group, storageVolume, RaftStorage.StartupOption.FORMAT,
        stateMachine, false)) {
      follower.start();
      final IOException failure = new IOException("Failed to persist metadata");
      Mockito.doThrow(failure).when(spyMetadataFile(follower)).persist(Mockito.any());
      Assertions.assertSame(failure, Assertions.assertThrows(IOException.class,
          () -> follower.appendEntries(appendEntries(group, 1, 0))));

      assertStopped(follower, group, stateMachine);
      callbackThread.get().join(5000);
      Assertions.assertFalse(callbackThread.get().isAlive());
      Assertions.assertNull(uncaught.get());
    }
  }

  @Test
  public void testNonMetadataExceptionDoesNotStopDivision() throws Exception {
    final RaftGroup group = RaftGroup.valueOf(RaftGroupId.randomId(),
        Arrays.asList(peer(LEADER), peer(FOLLOWER), peer(OTHER)));
    final RaftGroup otherGroup = RaftGroup.valueOf(RaftGroupId.randomId(), group.getPeers());
    final File storageVolume = new File(getTestDir(), "storage");
    FileUtils.deleteFully(storageVolume);

    try (RaftServerImpl follower = newServer(group, storageVolume, RaftStorage.StartupOption.FORMAT)) {
      follower.start();
      Assertions.assertThrows(GroupMismatchException.class,
          () -> follower.appendEntries(appendEntries(otherGroup, 1, 0)));
      Assertions.assertTrue(follower.getInfo().isAlive());
      Assertions.assertEquals(AppendResult.SUCCESS, follower.appendEntries(appendEntries(group, 1, 1)).getResult());
    }
  }

  private void runTestMetadataFailure(Op op, boolean rejectVote, boolean snapshotChunk) throws Exception {
    final RaftPeer followerPeer = RaftPeer.newBuilder()
        .setId(FOLLOWER).setAddress("127.0.0.1:0").setPriority(rejectVote ? 1 : 0).build();
    final RaftGroup group = RaftGroup.valueOf(RaftGroupId.randomId(),
        Arrays.asList(peer(LEADER), followerPeer, peer(OTHER)));
    final File storageVolume = new File(getTestDir(), "storage");
    FileUtils.deleteFully(storageVolume);
    final ShutdownStateMachine stateMachine = new ShutdownStateMachine();

    try (RaftServerImpl follower = newServer(group, storageVolume, RaftStorage.StartupOption.FORMAT,
        stateMachine, snapshotChunk)) {
      follower.start();
      final File metadataFile = new File(follower.getRaftStorage().getStorageDir().getCurrentDir(), "raft-meta");
      final File temporaryMetadataFile = AtomicFileOutputStream.getTemporaryFile(metadataFile);

      // Make raft-meta.tmp a directory so opening it for writing fails with IOException.
      Files.createDirectory(temporaryMetadataFile.toPath());
      try {
        Assertions.assertThrows(IOException.class, () -> {
          switch (op) {
            case APPEND_ENTRIES:
              follower.appendEntries(appendEntries(group, 1, 0));
              break;
            case REQUEST_VOTE:
              follower.requestVote(requestVote(group, LEADER, 1));
              break;
            case INSTALL_SNAPSHOT:
              follower.installSnapshot(installSnapshot(group, snapshotChunk));
              break;
            default:
              throw new IllegalArgumentException("Unexpected operation " + op);
          }
        });
        Assertions.assertFalse(follower.getInfo().isAlive());
        Assertions.assertEquals(1L, follower.getState().getCurrentTerm());
        Assertions.assertEquals(0L, loadPersistedTerm(follower));
      } finally {
        Files.delete(temporaryMetadataFile.toPath());
      }
      assertStopped(follower, group, stateMachine);
    }

    try (RaftServerImpl restarted = newServer(group, storageVolume, RaftStorage.StartupOption.RECOVER)) {
      restarted.start();
      Assertions.assertEquals(0L, restarted.getState().getCurrentTerm());
      Assertions.assertEquals(AppendResult.SUCCESS, restarted.appendEntries(appendEntries(group, 1, 1)).getResult());
      Assertions.assertEquals(1L, loadPersistedTerm(restarted));
    }
  }

  private static void assertStopped(RaftServerImpl server, RaftGroup group, ShutdownStateMachine stateMachine)
      throws Exception {
    Assertions.assertFalse(server.getInfo().isAlive());
    Assertions.assertThrows(ServerNotReadyException.class, () -> server.appendEntries(appendEntries(group, 1, 1)));
    Assertions.assertThrows(ServerNotReadyException.class, () -> server.requestVote(requestVote(group, OTHER, 1)));
    Assertions.assertFalse(stateMachine.shutdown.get(5, TimeUnit.SECONDS));
    Assertions.assertTrue(stateMachine.closedWhenNotified);
    Assertions.assertEquals(LifeCycle.State.CLOSED, server.getInfo().getLifeCycleState());
    server.close();
    Assertions.assertEquals(1, stateMachine.shutdownCount.get());
  }

  private static InstallSnapshotRequestProto installSnapshot(RaftGroup group, boolean snapshotChunk) {
    final InstallSnapshotRequestProto.Builder builder = InstallSnapshotRequestProto.newBuilder()
        .setServerRequest(appendEntries(group, 1, 0).getServerRequest())
        .setLeaderTerm(1);
    if (snapshotChunk) {
      builder.setSnapshotChunk(InstallSnapshotRequestProto.SnapshotChunkProto.newBuilder()
          .setRequestId(group.getGroupId().toString()).setRequestIndex(0)
          .setTermIndex(TermIndex.valueOf(1, 1).toProto()).setDone(true));
    } else {
      builder.setNotification(InstallSnapshotRequestProto.NotificationProto.newBuilder()
          .setFirstAvailableTermIndex(TermIndex.valueOf(1, 1).toProto()));
    }
    return builder.build();
  }

  private static class ShutdownStateMachine extends BaseStateMachine {
    private final CompletableFuture<Boolean> shutdown = new CompletableFuture<>();
    private final AtomicInteger shutdownCount = new AtomicInteger();
    private volatile boolean closed;
    private volatile boolean closedWhenNotified;

    @Override
    public void close() throws IOException {
      super.close();
      closed = true;
    }

    @Override
    public void notifyServerShutdown(RoleInfoProto roleInfo, boolean allServer) {
      closedWhenNotified = closed;
      shutdownCount.incrementAndGet();
      shutdown.complete(allServer);
    }
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

  private static RequestVoteRequestProto requestVote(RaftGroup group, RaftPeerId candidate, long term) {
    return ServerProtoUtils.toRequestVoteRequestProto(
        RaftGroupMemberId.valueOf(candidate, group.getGroupId()), FOLLOWER, term, null, false);
  }

  private static RaftPeer peer(RaftPeerId id) {
    return RaftPeer.newBuilder().setId(id).setAddress("127.0.0.1:0").build();
  }

  private static RaftServerImpl newServer(RaftGroup group, File storageVolume, RaftStorage.StartupOption option)
      throws IOException {
    return newServer(group, storageVolume, option, new BaseStateMachine(), false);
  }

  private static RaftServerImpl newServer(RaftGroup group, File storageVolume, RaftStorage.StartupOption option,
      StateMachine stateMachine, boolean installSnapshot) throws IOException {
    final RaftProperties properties = new RaftProperties();
    RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storageVolume));
    RaftServerConfigKeys.Log.Appender.setInstallSnapshotEnabled(properties, installSnapshot);
    RaftServerConfigKeys.Rpc.setTimeoutMin(properties, TimeDuration.valueOf(60, TimeUnit.SECONDS));
    RaftServerConfigKeys.Rpc.setTimeoutMax(properties, TimeDuration.valueOf(61, TimeUnit.SECONDS));
    RaftServerConfigKeys.Rpc.setFirstElectionTimeoutMin(properties, TimeDuration.valueOf(60, TimeUnit.SECONDS));
    RaftServerConfigKeys.Rpc.setFirstElectionTimeoutMax(properties, TimeDuration.valueOf(61, TimeUnit.SECONDS));

    final RaftServerProxy proxy = Mockito.mock(RaftServerProxy.class);
    Mockito.when(proxy.getId()).thenReturn(FOLLOWER);
    Mockito.when(proxy.getPeer()).thenReturn(group.getPeer(FOLLOWER));
    Mockito.when(proxy.getProperties()).thenReturn(properties);
    Mockito.when(proxy.getThreadGroup()).thenReturn(new ThreadGroup("metadata-persistence-" + option));
    Mockito.when(proxy.getServerRpc()).thenReturn(Mockito.mock(RaftServerRpc.class));
    return new RaftServerImpl(group, stateMachine, proxy, option);
  }

  private static AppendEntriesRequestProto appendEntries(RaftGroup group, long term, long callId) {
    return AppendEntriesRequestProto.newBuilder()
        .setServerRequest(RaftRpcRequestProto.newBuilder()
            .setRequestorId(LEADER.toByteString())
            .setReplyId(FOLLOWER.toByteString())
            .setRaftGroupId(RaftGroupIdProto.newBuilder().setId(group.getGroupId().toByteString()).build())
            .setCallId(callId)
            .build())
        .setLeaderTerm(term)
        .setLeaderCommit(0)
        .build();
  }

  private static long loadPersistedTerm(RaftServerImpl server) throws IOException {
    return server.getRaftStorage().getMetadataFile().getMetadata().getTerm();
  }
}
