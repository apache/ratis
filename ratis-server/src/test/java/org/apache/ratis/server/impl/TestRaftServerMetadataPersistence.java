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
import org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto.AppendResult;
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.proto.RaftProtos.RaftGroupIdProto;
import org.apache.ratis.proto.RaftProtos.RaftRpcRequestProto;
import org.apache.ratis.proto.RaftProtos.RequestVoteReplyProto;
import org.apache.ratis.proto.RaftProtos.RequestVoteRequestProto;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.RaftServerRpc;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.server.storage.RaftStorageMetadataFile;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.util.AtomicFileOutputStream;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.TimeDuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class TestRaftServerMetadataPersistence extends BaseTest {
  private static final RaftPeerId LEADER = RaftPeerId.valueOf("s1");
  private static final RaftPeerId FOLLOWER = RaftPeerId.valueOf("s2");
  private static final RaftPeerId OTHER = RaftPeerId.valueOf("s3");

  @Test
  public void testRetryMetadataPersistence() throws Exception {
    final RaftGroup group = RaftGroup.valueOf(RaftGroupId.randomId(),
        Arrays.asList(peer(LEADER), peer(FOLLOWER), peer(OTHER)));
    final File storageVolume = new File(getTestDir(), "storage");
    FileUtils.deleteFully(storageVolume);
    Files.createDirectories(storageVolume.toPath());

    RaftServerImpl follower = newServer(group, storageVolume, RaftStorage.StartupOption.FORMAT);
    RaftServerImpl restarted = null;
    try {
      follower.start();
      final RaftStorageMetadataFile metadata = spyMetadataFile(follower);
      final File metadataFile = new File(follower.getRaftStorage().getStorageDir().getCurrentDir(), "raft-meta");
      final File temporaryMetadataFile = AtomicFileOutputStream.getTemporaryFile(metadataFile);

      final AppendEntriesReplyProto initial = follower.appendEntries(appendEntries(group, 0, 0));
      Assertions.assertEquals(AppendResult.SUCCESS, initial.getResult());
      Assertions.assertEquals(0L, loadPersistedTerm(follower));

      Files.createDirectory(temporaryMetadataFile.toPath());
      try {
        final RaftServerImpl server = follower;
        Assertions.assertThrows(IOException.class, () -> server.appendEntries(appendEntries(group, 1, 1)));
        Assertions.assertThrows(IOException.class, () -> server.appendEntries(appendEntries(group, 1, 2)));
      } finally {
        Files.delete(temporaryMetadataFile.toPath());
      }

      Assertions.assertEquals(1L, follower.getState().getCurrentTerm());
      Assertions.assertEquals(0L, loadPersistedTerm(follower));

      final AppendEntriesReplyProto retry = follower.appendEntries(appendEntries(group, 1, 3));
      Assertions.assertEquals(AppendResult.SUCCESS, retry.getResult());
      Assertions.assertEquals(1L, loadPersistedTerm(follower));

      Mockito.clearInvocations(metadata);
      for (int i = 4; i < 7; i++) {
        Assertions.assertEquals(AppendResult.SUCCESS, follower.appendEntries(appendEntries(group, 1, i)).getResult());
      }
      Mockito.verify(metadata, Mockito.never()).persist(Mockito.any());

      follower.close();
      follower = null;

      restarted = newServer(group, storageVolume, RaftStorage.StartupOption.RECOVER);
      restarted.start();
      Assertions.assertEquals(1L, restarted.getState().getCurrentTerm());
    } finally {
      if (follower != null) {
        follower.close();
      }
      if (restarted != null) {
        restarted.close();
      }
    }
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
  public void testRetryMetadataPersistenceAfterRejectedVote() throws Exception {
    final RaftPeer followerPeer = RaftPeer.newBuilder()
        .setId(FOLLOWER).setAddress("127.0.0.1:0").setPriority(1).build();
    final RaftGroup group = RaftGroup.valueOf(RaftGroupId.randomId(),
        Arrays.asList(peer(LEADER), followerPeer, peer(OTHER)));
    final File storageVolume = new File(getTestDir(), "storage");
    FileUtils.deleteFully(storageVolume);

    try (RaftServerImpl follower = newServer(group, storageVolume, RaftStorage.StartupOption.FORMAT)) {
      follower.start();
      final File metadataFile = new File(follower.getRaftStorage().getStorageDir().getCurrentDir(), "raft-meta");
      final File temporaryMetadataFile = AtomicFileOutputStream.getTemporaryFile(metadataFile);
      final RequestVoteRequestProto vote = requestVote(group, LEADER, 1);

      Files.createDirectory(temporaryMetadataFile.toPath());
      try {
        Assertions.assertThrows(IOException.class, () -> follower.requestVote(vote));
        Assertions.assertThrows(IOException.class, () -> follower.requestVote(vote));
      } finally {
        Files.delete(temporaryMetadataFile.toPath());
      }
      Assertions.assertEquals(1L, follower.getState().getCurrentTerm());
      Assertions.assertEquals(0L, loadPersistedTerm(follower));

      final RequestVoteReplyProto retry = follower.requestVote(vote);
      Assertions.assertFalse(retry.getServerReply().getSuccess());
      Assertions.assertEquals(1L, loadPersistedTerm(follower));

      final RaftStorageMetadataFile metadata = spyMetadataFile(follower);
      Assertions.assertFalse(follower.requestVote(vote).getServerReply().getSuccess());
      Mockito.verify(metadata, Mockito.never()).persist(Mockito.any());
    }
  }

  @Test
  public void testRetryMetadataPersistenceAfterGrantedVote() throws Exception {
    final RaftGroup group = RaftGroup.valueOf(RaftGroupId.randomId(),
        Arrays.asList(peer(LEADER), peer(FOLLOWER), peer(OTHER)));
    final File storageVolume = new File(getTestDir(), "storage");
    FileUtils.deleteFully(storageVolume);

    try (RaftServerImpl follower = newServer(group, storageVolume, RaftStorage.StartupOption.FORMAT)) {
      follower.start();
      final File metadataFile = new File(follower.getRaftStorage().getStorageDir().getCurrentDir(), "raft-meta");
      final File temporaryMetadataFile = AtomicFileOutputStream.getTemporaryFile(metadataFile);

      Files.createDirectory(temporaryMetadataFile.toPath());
      try {
        Assertions.assertThrows(IOException.class, () -> follower.requestVote(requestVote(group, LEADER, 1)));
      } finally {
        Files.delete(temporaryMetadataFile.toPath());
      }
      Assertions.assertEquals(LEADER, follower.getState().getVotedFor());
      Assertions.assertEquals(0L, loadPersistedTerm(follower));

      Assertions.assertEquals(AppendResult.SUCCESS, follower.appendEntries(appendEntries(group, 1, 0)).getResult());
      Assertions.assertEquals(1L, loadPersistedTerm(follower));
      Assertions.assertEquals(LEADER, follower.getRaftStorage().getMetadataFile().getMetadata().getVotedFor());
    }

    try (RaftServerImpl restarted = newServer(group, storageVolume, RaftStorage.StartupOption.RECOVER)) {
      restarted.start();
      Assertions.assertEquals(1L, restarted.getState().getCurrentTerm());
      Assertions.assertEquals(LEADER, restarted.getState().getVotedFor());
      Assertions.assertFalse(restarted.requestVote(requestVote(group, OTHER, 1)).getServerReply().getSuccess());
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
    final RaftProperties properties = new RaftProperties();
    RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storageVolume));
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
    return new RaftServerImpl(group, new BaseStateMachine(), proxy, option);
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
