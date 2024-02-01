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

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.server.impl.RaftServerTestUtil;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.impl.FileListSnapshotInfo;
import org.apache.ratis.statemachine.impl.SimpleStateMachine4Testing;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.SizeInBytes;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public abstract class InstallSnapshotFromLeaderTests<CLUSTER extends MiniRaftCluster>
    extends BaseTest
    implements MiniRaftCluster.Factory.Get<CLUSTER> {
  static final Logger LOG = LoggerFactory.getLogger(InstallSnapshotFromLeaderTests.class);

  {
    final RaftProperties prop = getProperties();
    RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(prop, true);
    RaftServerConfigKeys.Snapshot.setAutoTriggerThreshold(prop, SNAPSHOT_TRIGGER_THRESHOLD);
    RaftServerConfigKeys.Log.setPurgeGap(prop, PURGE_GAP);
    RaftServerConfigKeys.Log.Appender.setSnapshotChunkSizeMax(prop, SizeInBytes.ONE_KB);
    RaftServerConfigKeys.LeaderElection.setMemberMajorityAdd(prop, true);
  }

  private static final int SNAPSHOT_TRIGGER_THRESHOLD = 64;
  private static final int PURGE_GAP = 8;

  public void testMultiFileInstallSnapshot() throws Exception {
    getProperties().setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        StateMachineWithMultiNestedSnapshotFile.class, StateMachine.class);
    runWithNewCluster(1, this::testMultiFileInstallSnapshot);
  }

  public void testSeparateSnapshotInstallPath() throws Exception {
    getProperties().setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        StateMachineWithSeparatedSnapshotPath.class, StateMachine.class);
    runWithNewCluster(1, this::testMultiFileInstallSnapshot);
  }

  private void testMultiFileInstallSnapshot(CLUSTER cluster) throws Exception {
    try {
      int i = 0;
      RaftTestUtil.waitForLeader(cluster);
      final RaftPeerId leaderId = cluster.getLeader().getId();

      try (final RaftClient client = cluster.createClient(leaderId)) {
        for (; i < SNAPSHOT_TRIGGER_THRESHOLD * 2 - 1; i++) {
          RaftClientReply
              reply = client.io().send(new RaftTestUtil.SimpleMessage("m" + i));
          Assertions.assertTrue(reply.isSuccess());
        }

        client.getSnapshotManagementApi(leaderId).create(3000);
      }

      final SnapshotInfo snapshot = cluster.getLeader().getStateMachine().getLatestSnapshot();
      Assertions.assertEquals(3, snapshot.getFiles().size());

      // add two more peers
      final MiniRaftCluster.PeerChanges change = cluster.addNewPeers(2, true,
          true);
      // trigger setConfiguration
      cluster.setConfiguration(change.allPeersInNewConf);

      RaftServerTestUtil
          .waitAndCheckNewConf(cluster, change.allPeersInNewConf, 0, null);

      // Check the installed snapshot file number on each Follower matches with the
      // leader snapshot.
      JavaUtils.attempt(() -> {
        for (RaftServer.Division follower : cluster.getFollowers()) {
          final SnapshotInfo info = follower.getStateMachine().getLatestSnapshot();
          Assertions.assertNotNull(info);
          Assertions.assertEquals(3, info.getFiles().size());
        }
      }, 10, ONE_SECOND, "check snapshot", LOG);
    } finally {
      cluster.shutdown();
    }
  }

  private static class StateMachineWithMultiNestedSnapshotFile extends SimpleStateMachine4Testing {

    File snapshotRoot;
    File file1;
    File file2;

    @Override
    public synchronized void initialize(RaftServer server, RaftGroupId groupId, RaftStorage raftStorage) throws IOException {
      super.initialize(server, groupId, raftStorage);

            // contains two snapshot files
            // sm/snapshot/1.bin
            // sm/snapshot/sub/2.bin
            snapshotRoot = new File(getStateMachineDir(), "snapshot");
            file1 = new File(snapshotRoot, "1.bin");
            file2 = new File(new File(snapshotRoot, "sub"), "2.bin");
        }

    @Override
    public synchronized void pause() {
      if (getLifeCycle().getCurrentState() == LifeCycle.State.RUNNING) {
        getLifeCycle().transition(LifeCycle.State.PAUSING);
        getLifeCycle().transition(LifeCycle.State.PAUSED);
      }
    }

    @Override
    public long takeSnapshot() {
      final TermIndex termIndex = getLastAppliedTermIndex();
      if (termIndex.getTerm() <= 0 || termIndex.getIndex() <= 0) {
        return RaftLog.INVALID_LOG_INDEX;
      }

      final long endIndex = termIndex.getIndex();
      try {
        if (!snapshotRoot.exists()) {
          FileUtils.createDirectories(snapshotRoot);
          FileUtils.createDirectories(file1.getParentFile());
          FileUtils.createDirectories(file2.getParentFile());
          FileUtils.newOutputStream(file1, StandardOpenOption.CREATE_NEW).close();
          // write 4KB data to simulate multiple chunk scene
          final byte[] data = new byte[4096];
          Arrays.fill(data, (byte)0x01);
          try (OutputStream fout = FileUtils.newOutputStream(file2, StandardOpenOption.CREATE_NEW)) {
              fout.write(data);
          }
        }

      } catch (IOException ioException) {
        return RaftLog.INVALID_LOG_INDEX;
      }

      Assertions.assertTrue(file1.exists());
      Assertions.assertTrue(file2.exists());
      return super.takeSnapshot();
    }

    @Override
    public SnapshotInfo getLatestSnapshot() {
      if (!snapshotRoot.exists() || !file1.exists() || !file2.exists()) {
        return null;
      }
      List<FileInfo> files = new ArrayList<>();
      files.add(new FileInfo(
          file1.toPath(),
          null));
      files.add(new FileInfo(
          file2.toPath(),
          null));
      Assertions.assertEquals(2, files.size());

      SnapshotInfo info = super.getLatestSnapshot();
      if (info == null) {
        return null;
      }
      files.add(info.getFiles().get(0));
      return new FileListSnapshotInfo(files,
          info.getTerm(), info.getIndex());
    }
  }


  private static class StateMachineWithSeparatedSnapshotPath extends SimpleStateMachine4Testing {
    private File root;
    private File snapshotDir;
    private File tmpDir;

    @Override
    public synchronized void initialize(RaftServer server, RaftGroupId groupId, RaftStorage raftStorage) throws IOException {
      super.initialize(server, groupId, raftStorage);
      this.root = new File("/tmp/ratis-tests/statemachine/" + getId().toString());
      this.snapshotDir = new File(root, "snapshot");
      this.tmpDir = new File(root, "tmp");
      FileUtils.deleteFully(root);
      Assertions.assertTrue(this.snapshotDir.mkdirs());
      Assertions.assertTrue(this.tmpDir.mkdirs());
      this.root.deleteOnExit();
    }

    @Override
    public synchronized void pause() {
      if (getLifeCycle().getCurrentState() == LifeCycle.State.RUNNING) {
        getLifeCycle().transition(LifeCycle.State.PAUSING);
        getLifeCycle().transition(LifeCycle.State.PAUSED);
      }
    }

    @Override
    public long takeSnapshot() {
      final TermIndex lastApplied = getLastAppliedTermIndex();
      final File snapshotTmpDir = new File(tmpDir, UUID.randomUUID().toString());
      final File snapshotRealDir = new File(snapshotDir, String.format("%d_%d", lastApplied.getTerm(), lastApplied.getIndex()));

      try {
        FileUtils.deleteFully(snapshotRealDir);
        FileUtils.deleteFully(snapshotTmpDir);
        Assertions.assertTrue(snapshotTmpDir.mkdirs());
        final File snapshotFile1 = new File(snapshotTmpDir, "deer");
        final File snapshotFile2 = new File(snapshotTmpDir, "loves");
        final File snapshotFile3 = new File(snapshotTmpDir, "vegetable");
        Assertions.assertTrue(snapshotFile1.createNewFile());
        Assertions.assertTrue(snapshotFile2.createNewFile());
        Assertions.assertTrue(snapshotFile3.createNewFile());
        FileUtils.move(snapshotTmpDir, snapshotRealDir);
      } catch (IOException ioe) {
        LOG.error("create snapshot data file failed", ioe);
        return RaftLog.INVALID_LOG_INDEX;
      }

      return lastApplied.getIndex();
    }

    @Override
    public SnapshotInfo getLatestSnapshot() {
      Path[] sortedSnapshots = getSortedSnapshotDirPaths();
      if (sortedSnapshots == null || sortedSnapshots.length == 0) {
        return null;
      }

      File latest = sortedSnapshots[sortedSnapshots.length - 1].toFile();
      TermIndex snapshotLastIncluded = TermIndex.valueOf
          (Long.parseLong(latest.getName().split("_")[0]), Long.parseLong(latest.getName().split("_")[1]));

      List<FileInfo> fileInfos = new ArrayList<>();
      for (File f : Objects.requireNonNull(latest.listFiles())) {
        if (!f.getName().endsWith(".md5")) {
          fileInfos.add(new FileInfo(f.toPath(), null));
        }
      }

      return new FileListSnapshotInfo(fileInfos, snapshotLastIncluded.getTerm(), snapshotLastIncluded.getIndex());
    }

    private Path[] getSortedSnapshotDirPaths() {
      ArrayList<Path> snapshotPaths = new ArrayList<>();
      try (DirectoryStream<Path> stream = Files.newDirectoryStream(snapshotDir.toPath())) {
        for (Path path : stream) {
          if (path.toFile().isDirectory()) {
            snapshotPaths.add(path);
          }
        }
      } catch (IOException exception) {
        LOG.warn("cannot construct snapshot directory stream ", exception);
        return null;
      }

      Path[] pathArray = snapshotPaths.toArray(new Path[0]);
      Arrays.sort(
          pathArray,
          (o1, o2) -> {
            String index1 = o1.toFile().getName().split("_")[1];
            String index2 = o2.toFile().getName().split("_")[1];
            return Long.compare(Long.parseLong(index1), Long.parseLong(index2));
          });
      return pathArray;
    }

    @Override
    public SimpleStateMachineStorage getStateMachineStorage() {
      return new SeparateSnapshotStateMachineStorage();
    }

    private class SeparateSnapshotStateMachineStorage extends SimpleStateMachineStorage {
      @Override
      public File getSnapshotDir() {
        return snapshotDir;
      }

      @Override
      public File getTmpDir() {
        return tmpDir;
      }
    }
  }
}
