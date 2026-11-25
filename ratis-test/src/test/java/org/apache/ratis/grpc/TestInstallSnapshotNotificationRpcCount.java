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
package org.apache.ratis.grpc;

import org.apache.ratis.BaseTest;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.server.GrpcServicesImpl;
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotRequestProto;
import org.apache.ratis.proto.RaftProtos.RoleInfoProto;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.server.impl.RaftServerTestUtil;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachine4Testing;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.util.CodeInjectionForTesting;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * While a follower is installing a snapshot through its state machine (installSnapshotEnabled = false),
 * the leader should send at most about one notification and one heartbeat per heartbeat interval,
 * even if a client keeps writing.
 */
public class TestInstallSnapshotNotificationRpcCount extends BaseTest {
  /** The same as RaftServerImpl.INSTALL_SNAPSHOT, which is package-private. */
  private static final String INSTALL_SNAPSHOT_INJECTION = "RaftServerImpl.installSnapshot";

  private static final int SNAPSHOT_TRIGGER_THRESHOLD = 64;
  private static final TimeDuration INSTALL_DELAY = TimeDuration.valueOf(3, TimeUnit.SECONDS);

  /** The leader snapshot to install; it is taken by the first notification only. */
  private static final AtomicReference<SnapshotInfo> LEADER_SNAPSHOT = new AtomicReference<>();
  private static final AtomicBoolean INSTALLING = new AtomicBoolean();
  private static final AtomicLong INSTALL_START_NANOS = new AtomicLong();
  private static final AtomicLong INSTALL_END_NANOS = new AtomicLong();

  // Counted only while INSTALLING.
  private static final AtomicLong NOTIFICATIONS = new AtomicLong();
  private static final AtomicLong HEARTBEATS = new AtomicLong();
  private static final AtomicLong APPEND_ENTRIES_WITH_ENTRIES = new AtomicLong();
  private static final AtomicLong CLIENT_WRITES = new AtomicLong();

  /** Install the leader snapshot slowly, by copying the snapshot files after {@link #INSTALL_DELAY}. */
  public static class SlowInstallStateMachine extends SimpleStateMachine4Testing {
    @Override
    public CompletableFuture<TermIndex> notifyInstallSnapshotFromLeader(RoleInfoProto roleInfo, TermIndex first) {
      // With client writes, the leader keeps purging its log so that the follower may be notified again
      // after the first installation.  Only the first installation is slow and measured.
      final SingleFileSnapshotInfo leaderSnapshot = (SingleFileSnapshotInfo) LEADER_SNAPSHOT.getAndSet(null);
      if (leaderSnapshot == null) {
        return super.notifyInstallSnapshotFromLeader(roleInfo, first);
      }
      return CompletableFuture.supplyAsync(() -> {
        INSTALL_START_NANOS.set(System.nanoTime());
        INSTALLING.set(true);
        try {
          INSTALL_DELAY.sleep();
          final Path src = leaderSnapshot.getFile().getPath();
          copy(src);
          copy(src.resolveSibling(src.getFileName() + ".md5")); // a snapshot without md5 is ignored
          getStateMachineStorage().loadLatestSnapshot(); // refresh the cached latest snapshot
          return leaderSnapshot.getTermIndex();
        } catch (Exception e) {
          throw new IllegalStateException("Failed to install snapshot", e);
        } finally {
          INSTALLING.set(false);
          INSTALL_END_NANOS.set(System.nanoTime());
        }
      });
    }

    private void copy(Path src) throws Exception {
      final File dst = new File(getStateMachineDir(), src.getFileName().toString());
      if (src.toFile().exists() && !dst.exists()) {
        Files.copy(src, dst.toPath());
      }
    }
  }

  private static RaftProperties newProperties() {
    final RaftProperties p = new RaftProperties();
    p.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY, SlowInstallStateMachine.class, StateMachine.class);
    RaftServerConfigKeys.Log.Appender.setInstallSnapshotEnabled(p, false);
    RaftServerConfigKeys.Snapshot.setAutoTriggerThreshold(p, SNAPSHOT_TRIGGER_THRESHOLD);
    RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(p, true);
    RaftServerConfigKeys.Log.setPurgeGap(p, 8);
    // Purge the log even though a follower is down, so that it has to install a snapshot.
    RaftServerConfigKeys.Log.setPurgeUptoSnapshotIndex(p, true);
    RaftServerConfigKeys.Log.setSegmentSizeMax(p, SizeInBytes.valueOf(1024));
    return p;
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS)
  public void testRpcCountDuringInstallSnapshotNotification() throws Exception {
    LEADER_SNAPSHOT.set(null);
    INSTALLING.set(false);
    INSTALL_END_NANOS.set(0);
    for (AtomicLong c : new AtomicLong[] {NOTIFICATIONS, HEARTBEATS, APPEND_ENTRIES_WITH_ENTRIES, CLIENT_WRITES}) {
      c.set(0);
    }

    try (MiniRaftClusterWithGrpc cluster = new MiniRaftClusterWithGrpc(
        MiniRaftCluster.generateIds(3, 0), newProperties(), null)) {
      cluster.start();
      final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster);
      writeMessages(cluster, leader, SNAPSHOT_TRIGGER_THRESHOLD * 2);

      // Kill a follower and write more, so that the leader purges the log entries the follower needs.
      final RaftPeerId followerId = cluster.getFollowers().get(0).getId();
      final long followerNextIndex = cluster.getDivision(followerId).getRaftLog().getNextIndex();
      cluster.killServer(followerId);
      writeMessages(cluster, leader, SNAPSHOT_TRIGGER_THRESHOLD * 2);
      JavaUtils.attempt(() -> Assertions.assertTrue(leader.getRaftLog().getStartIndex() > followerNextIndex),
          20, ONE_SECOND, "leader purged", LOG);
      final SnapshotInfo leaderSnapshot = leader.getStateMachine().getLatestSnapshot();
      LEADER_SNAPSHOT.set(leaderSnapshot);

      CodeInjectionForTesting.put(GrpcServicesImpl.GRPC_SEND_SERVER_REQUEST, (local, remote, args) -> {
        if (INSTALLING.get() && leader.getId().equals(local) && args[0] instanceof AppendEntriesRequestProto) {
          final AppendEntriesRequestProto r = (AppendEntriesRequestProto) args[0];
          if (followerId.toByteString().equals(r.getServerRequest().getReplyId())) {
            (r.getEntriesCount() == 0 ? HEARTBEATS : APPEND_ENTRIES_WITH_ENTRIES).incrementAndGet();
          }
        }
        return false;
      });
      CodeInjectionForTesting.put(INSTALL_SNAPSHOT_INJECTION, (local, remote, args) -> {
        if (INSTALLING.get() && followerId.equals(local) && ((InstallSnapshotRequestProto) args[0]).hasNotification()) {
          NOTIFICATIONS.incrementAndGet();
        }
        return false;
      });

      // Keep writing during the installation: each new entry wakes up the LogAppenders.
      final AtomicBoolean stopWriting = new AtomicBoolean();
      final Thread writer = new Thread(() -> {
        try (RaftClient client = cluster.createClient(leader.getId())) {
          for (int i = 0; !stopWriting.get(); i++) {
            if (client.io().send(new RaftTestUtil.SimpleMessage("w" + i)).isSuccess() && INSTALLING.get()) {
              CLIENT_WRITES.incrementAndGet();
            }
          }
        } catch (Exception e) {
          LOG.info("writer stopped", e);
        }
      }, "writer");
      writer.setDaemon(true);

      try {
        writer.start();
        // Restart the follower; it has to install the leader snapshot via notification.
        cluster.restartServer(followerId, false);
        // Only wait until the leader learns that the snapshot is installed: SimpleStateMachine4Testing
        // cannot reload a snapshot on top of an existing state, so the follower is not checked for catching up.
        JavaUtils.attempt(() -> {
          Assertions.assertTrue(INSTALL_END_NANOS.get() > 0, "install not finished");
          Assertions.assertTrue(RaftServerTestUtil.getLogAppenders(leader)
              .filter(a -> a.getFollowerId().equals(followerId))
              .anyMatch(a -> a.getFollower().getSnapshotIndex() >= leaderSnapshot.getIndex()), "leader not informed");
        }, 60, ONE_SECOND, "snapshot installed", LOG);
      } finally {
        stopWriting.set(true);
        writer.join(10_000);
        CodeInjectionForTesting.remove(GrpcServicesImpl.GRPC_SEND_SERVER_REQUEST);
        CodeInjectionForTesting.remove(INSTALL_SNAPSHOT_INJECTION);
      }

      final long installMs = TimeUnit.NANOSECONDS.toMillis(INSTALL_END_NANOS.get() - INSTALL_START_NANOS.get());
      final long heartbeatMs = RaftServerConfigKeys.Rpc.timeoutMin(cluster.getProperties())
          .toLong(TimeUnit.MILLISECONDS) / 2;
      LOG.info("During the {}ms installation ({} client writes): {} notifications and {} heartbeats"
          + " (heartbeat interval {}ms)", installMs, CLIENT_WRITES.get(), NOTIFICATIONS.get(), HEARTBEATS.get(),
          heartbeatMs);

      Assertions.assertTrue(installMs >= INSTALL_DELAY.toLong(TimeUnit.MILLISECONDS), "the installation was not slow");
      Assertions.assertTrue(CLIENT_WRITES.get() > 0, "no client writes during the installation");
      Assertions.assertEquals(0, APPEND_ENTRIES_WITH_ENTRIES.get(), "sent log entries during the installation");
      // At most about one notification and one heartbeat per heartbeat interval (with 3x slack).
      final long rpcs = NOTIFICATIONS.get() + HEARTBEATS.get();
      final long maxRpcs = 3 * installMs / heartbeatMs;
      Assertions.assertTrue(rpcs <= maxRpcs, () -> "Too many RPCs during the installation: " + NOTIFICATIONS.get()
          + " notifications + " + HEARTBEATS.get() + " heartbeats = " + rpcs + " > " + maxRpcs);
    }
  }

  private static void writeMessages(MiniRaftClusterWithGrpc cluster, RaftServer.Division leader, int n)
      throws Exception {
    try (RaftClient client = cluster.createClient(leader.getId())) {
      for (int i = 0; i < n; i++) {
        Assertions.assertTrue(client.io().send(new RaftTestUtil.SimpleMessage("m" + i)).isSuccess());
      }
    }
  }
}
