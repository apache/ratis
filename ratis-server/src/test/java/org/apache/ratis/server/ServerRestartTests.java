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
package org.apache.ratis.server;

import org.apache.log4j.Level;
import org.apache.ratis.BaseTest;
import org.apache.ratis.MiniRaftCluster;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.RaftTestUtil.SimpleMessage;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.impl.RaftServerProxy;
import org.apache.ratis.server.impl.ServerProtoUtils;
import org.apache.ratis.server.impl.ServerState;
import org.apache.ratis.server.storage.RaftLog;
import org.apache.ratis.server.storage.RaftStorageDirectory.LogPathAndIndex;
import org.apache.ratis.server.storage.SegmentedRaftLogFormat;
import org.apache.ratis.statemachine.SimpleStateMachine4Testing;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.LogUtils;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.StringUtils;
import org.apache.ratis.util.TimeDuration;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Test restarting raft peers.
 */
public abstract class ServerRestartTests<CLUSTER extends MiniRaftCluster>
    extends BaseTest
    implements MiniRaftCluster.Factory.Get<CLUSTER> {
  {
    LogUtils.setLogLevel(RaftLog.LOG, Level.DEBUG);
  }

  static final int NUM_SERVERS = 3;

  {
    final RaftProperties prop = getProperties();
    prop.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        SimpleStateMachine4Testing.class, StateMachine.class);
    RaftServerConfigKeys.Log.setSegmentSizeMax(prop, SizeInBytes.valueOf("8KB"));
  }

  @Test
  public void testRestartFollower() throws Exception {
    runWithNewCluster(NUM_SERVERS, this::runTestRestartFollower);
  }

  void runTestRestartFollower(MiniRaftCluster cluster) throws Exception {
    RaftTestUtil.waitForLeader(cluster);
    final RaftPeerId leaderId = cluster.getLeader().getId();

    // write some messages
    final AtomicInteger messageCount = new AtomicInteger();
    final Supplier<Message> newMessage = () -> new SimpleMessage("m" + messageCount.getAndIncrement());
    writeSomething(newMessage, cluster);

    // restart a follower
    RaftPeerId followerId = cluster.getFollowers().get(0).getId();
    LOG.info("Restart follower {}", followerId);
    cluster.restartServer(followerId, false);

    // write some more messages
    writeSomething(newMessage, cluster);
    final int truncatedMessageIndex = messageCount.get() - 1;

    final long leaderLastIndex = cluster.getLeader().getState().getLog().getLastEntryTermIndex().getIndex();
    // make sure the restarted follower can catchup
    final ServerState followerState = cluster.getRaftServerImpl(followerId).getState();
    JavaUtils.attempt(() -> followerState.getLastAppliedIndex() >= leaderLastIndex,
        10, 500, "follower catchup", LOG);

    // make sure the restarted peer's log segments is correct
    final RaftServerImpl follower = cluster.restartServer(followerId, false);
    final RaftLog followerLog = follower.getState().getLog();
    final long followerLastIndex = followerLog.getLastEntryTermIndex().getIndex();
    Assert.assertTrue(followerLastIndex >= leaderLastIndex);

    final File followerOpenLogFile = getOpenLogFile(follower);
    final File leaderOpenLogFile = getOpenLogFile(cluster.getRaftServerImpl(leaderId));

    // shutdown all servers
    cluster.getServers().forEach(RaftServerProxy::close);

    // truncate log and
    assertTruncatedLog(followerId, followerOpenLogFile, followerLastIndex, cluster);
    assertTruncatedLog(leaderId, leaderOpenLogFile, leaderLastIndex, cluster);

    // restart and write something.
    cluster.restart(false);
    writeSomething(newMessage, cluster);

    // restart again and check messages.
    cluster.restart(false);
    try(final RaftClient client = cluster.createClient()) {
      for(int i = 0; i < messageCount.get(); i++) {
        if (i != truncatedMessageIndex) {
          final Message m = new SimpleMessage("m" + i);
          final RaftClientReply reply = client.sendReadOnly(m);
          Assert.assertTrue(reply.isSuccess());
          LOG.info("query {}: {} {}", m, reply, LogEntryProto.parseFrom(reply.getMessage().getContent()));
        }
      }
    }
  }

  static void writeSomething(Supplier<Message> newMessage, MiniRaftCluster cluster) throws Exception {
    try(final RaftClient client = cluster.createClient()) {
      // write some messages
      for(int i = 0; i < 10; i++) {
        Assert.assertTrue(client.send(newMessage.get()).isSuccess());
      }
    }
  }

  static void assertTruncatedLog(RaftPeerId id, File openLogFile, long lastIndex, MiniRaftCluster cluster) throws Exception {
    // truncate log
    FileUtils.truncateFile(openLogFile, openLogFile.length() - 1);
    final RaftServerImpl server = cluster.restartServer(id, false);
    // the last index should be one less than before
    Assert.assertEquals(lastIndex - 1, server.getState().getLog().getLastEntryTermIndex().getIndex());
    server.getProxy().close();
  }

  static List<Path> getOpenLogFiles(RaftServerImpl server) throws Exception {
    return server.getState().getStorage().getStorageDir().getLogSegmentFiles().stream()
        .filter(LogPathAndIndex::isOpen)
        .map(LogPathAndIndex::getPath)
        .collect(Collectors.toList());
  }

  static File getOpenLogFile(RaftServerImpl server) throws Exception {
    final List<Path> openLogs = getOpenLogFiles(server);
    Assert.assertEquals(1, openLogs.size());
    return openLogs.get(0).toFile();
  }

  @Test
  public void testRestartWithCorruptedLogHeader() throws Exception {
    runWithNewCluster(NUM_SERVERS, this::runTestRestartWithCorruptedLogHeader);
  }

  void runTestRestartWithCorruptedLogHeader(MiniRaftCluster cluster) throws Exception {
    RaftTestUtil.waitForLeader(cluster);
    for(RaftServerImpl impl : cluster.iterateServerImpls()) {
      JavaUtils.attempt(() -> getOpenLogFile(impl), 10, TimeDuration.valueOf(100, TimeUnit.MILLISECONDS),
          impl.getId() + ": wait for log file creation", LOG);
    }

    // shutdown all servers
    cluster.getServers().forEach(RaftServerProxy::close);

    for(RaftServerImpl impl : cluster.iterateServerImpls()) {
      final File openLogFile = JavaUtils.attempt(() -> getOpenLogFile(impl),
          10, 100, impl.getId() + "-getOpenLogFile", LOG);
      for(int i = 0; i < SegmentedRaftLogFormat.getHeaderLength(); i++) {
        assertCorruptedLogHeader(impl.getId(), openLogFile, i, cluster, LOG);
        Assert.assertTrue(getOpenLogFiles(impl).isEmpty());
      }
    }
  }

  static void assertCorruptedLogHeader(RaftPeerId id, File openLogFile, int partialLength,
      MiniRaftCluster cluster, Logger LOG) throws Exception {
    Preconditions.assertTrue(partialLength < SegmentedRaftLogFormat.getHeaderLength());
    try(final RandomAccessFile raf = new RandomAccessFile(openLogFile, "rw")) {
      SegmentedRaftLogFormat.applyHeaderTo(header -> {
        LOG.info("header    = {}", StringUtils.bytes2HexString(header));
        final byte[] corrupted = new byte[header.length];
        System.arraycopy(header, 0, corrupted, 0, partialLength);
        LOG.info("corrupted = {}", StringUtils.bytes2HexString(corrupted));
        raf.write(corrupted);
        return null;
      });
    }
    final RaftServerImpl server = cluster.restartServer(id, false);
    server.getProxy().close();
  }

  @Test
  public void testRestartCommitIndex() throws Exception {
    runWithNewCluster(NUM_SERVERS, this::runTestRestartCommitIndex);
  }

  void runTestRestartCommitIndex(MiniRaftCluster cluster) throws Exception {
    final TimeDuration sleepTime = TimeDuration.valueOf(100, TimeUnit.MILLISECONDS);
    final SimpleMessage[] messages = SimpleMessage.create(10);
    try (final RaftClient client = cluster.createClient()) {
      for(SimpleMessage m : messages) {
        Assert.assertTrue(client.send(m).isSuccess());
      }
    }

    final List<RaftPeerId> ids = new ArrayList<>();
    final RaftLog leaderLog = cluster.getLeader().getState().getLog();
    final RaftPeerId leaderId = leaderLog.getSelfId();
    ids.add(leaderId);

    // check that the last logged commit index is equal to the index of the last committed StateMachineLogEntry
    final long lastIndex = leaderLog.getLastEntryTermIndex().getIndex();
    LOG.info("{}: leader lastIndex={}", leaderId, lastIndex);
    JavaUtils.attempt(() -> leaderLog.getLastCommittedIndex() == lastIndex,
        10, sleepTime, "leader(commitIndex == lastIndex)", LOG);

    final LogEntryProto lastEntry = leaderLog.get(lastIndex);
    LOG.info("{}: leader lastEntry entry[{}] = {}", leaderId, lastIndex, ServerProtoUtils.toLogEntryString(lastEntry));
    Assert.assertTrue(lastEntry.hasMetadataEntry());
    final long loggedCommitIndex = lastEntry.getMetadataEntry().getCommitIndex();
    for(long i = lastIndex - 1; i > loggedCommitIndex; i--) {
      final LogEntryProto entry = leaderLog.get(i);
      LOG.info("{}: leader entry[{}] =  {}", leaderId, i, ServerProtoUtils.toLogEntryString(entry));
      Assert.assertFalse(entry.hasStateMachineLogEntry());
    }
    final LogEntryProto lastCommittedEntry = leaderLog.get(loggedCommitIndex);
    LOG.info("{}: leader lastCommittedEntry = entry[{}] = {}",
        leaderId, loggedCommitIndex, ServerProtoUtils.toLogEntryString(lastCommittedEntry));
    Assert.assertTrue(lastCommittedEntry.hasStateMachineLogEntry());

    // check follower logs
    for(RaftServerImpl s : cluster.iterateServerImpls()) {
      if (!s.getId().equals(leaderId)) {
        ids.add(s.getId());
        RaftTestUtil.assertSameLog(leaderLog, s.getState().getLog());
      }
    }

    // kill all servers
    ids.forEach(cluster::killServer);

    // Restart and kill servers one by one so that they won't talk to each other.
    for(RaftPeerId id : ids) {
      cluster.restartServer(id, false);
      final RaftServerImpl server = cluster.getRaftServerImpl(id);
      final RaftLog raftLog = server.getState().getLog();
      JavaUtils.attempt(() -> raftLog.getLastCommittedIndex() >= loggedCommitIndex,
          10, sleepTime, id + "(commitIndex >= loggedCommitIndex)", LOG);
      JavaUtils.attempt(() -> server.getState().getLastAppliedIndex() >= loggedCommitIndex,
          10, sleepTime, id + "(lastAppliedIndex >= loggedCommitIndex)", LOG);
      LOG.info("{}: commitIndex={}, lastAppliedIndex={}",
          id, raftLog.getLastCommittedIndex(), server.getState().getLastAppliedIndex());
      cluster.killServer(id);
    }
  }
}
