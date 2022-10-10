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
import org.apache.ratis.statemachine.impl.SimpleStateMachine4Testing;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.impl.FileListSnapshotInfo;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.LifeCycle;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class InstallSnapshotFromLeaderTests<CLUSTER extends MiniRaftCluster>
        extends BaseTest
        implements MiniRaftCluster.Factory.Get<CLUSTER> {
    static final Logger LOG = LoggerFactory.getLogger(InstallSnapshotFromLeaderTests.class);
    {
        final RaftProperties prop = getProperties();
        prop.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
                StateMachineWithMultiNestedSnapshotFile.class, StateMachine.class);
        RaftServerConfigKeys.Log.setPurgeGap(prop, PURGE_GAP);
        RaftServerConfigKeys.Snapshot.setAutoTriggerThreshold(
                prop, SNAPSHOT_TRIGGER_THRESHOLD);
        RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(prop, true);
    }

    private static final int SNAPSHOT_TRIGGER_THRESHOLD = 64;
    private static final int PURGE_GAP = 8;

    @Test
    public void testMultiFileInstallSnapshot() throws Exception {
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
                    Assert.assertTrue(reply.isSuccess());
                }

                client.getSnapshotManagementApi(leaderId).create(3000);
            }

            final SnapshotInfo snapshot = cluster.getLeader().getStateMachine().getLatestSnapshot();
            Assert.assertEquals(3, snapshot.getFiles().size());

            // add two more peers
            final MiniRaftCluster.PeerChanges change = cluster.addNewPeers(2, true,
                    true);
            // trigger setConfiguration
            cluster.setConfiguration(change.allPeersInNewConf);

            RaftServerTestUtil
                    .waitAndCheckNewConf(cluster, change.allPeersInNewConf, 0, null);

            // Check the installed snapshot file number on each Follower matches with the
            // leader snapshot.
            for (RaftServer.Division follower : cluster.getFollowers()) {
                Assert.assertEquals(follower.getStateMachine().getLatestSnapshot().getFiles().size(), 3);
            }
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
                    FileUtils.createNewFile(file1.toPath());
                    FileUtils.createNewFile(file2.toPath());
                }

            } catch (IOException ioException) {
                return RaftLog.INVALID_LOG_INDEX;
            }

            Assert.assertTrue(file1.exists());
            Assert.assertTrue(file2.exists());
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
            Assert.assertEquals(files.size(), 2);

            SnapshotInfo info = super.getLatestSnapshot();
            if (info == null) {
                return null;
            }
            files.add(info.getFiles().get(0));
            return new FileListSnapshotInfo(files,
                    info.getTerm(), info.getIndex());
        }
    }
}
