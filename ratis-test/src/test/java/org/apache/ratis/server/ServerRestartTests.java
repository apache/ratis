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
package org.apache.ratis.server;

import org.apache.log4j.Level;
import org.apache.ratis.BaseTest;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.RaftTestUtil.SimpleMessage;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.protocol.exceptions.ChecksumException;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.LogProtoUtils;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.raftlog.RaftLogIOException;
import org.apache.ratis.server.raftlog.segmented.SegmentedRaftLogFormat;
import org.apache.ratis.server.RaftServerConfigKeys.Log;
import org.apache.ratis.server.raftlog.segmented.TestSegmentedRaftLog;
import org.apache.ratis.server.raftlog.segmented.LogSegmentPath;
import org.apache.ratis.statemachine.SimpleStateMachine4Testing;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Log4jUtils;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.StringUtils;
import org.apache.ratis.util.TimeDuration;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
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
    Log4jUtils.setLogLevel(RaftLog.LOG, Level.DEBUG);
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

    final long leaderLastIndex = cluster.getLeader().getRaftLog().getLastEntryTermIndex().getIndex();
    // make sure the restarted follower can catchup
    final RaftServer.Division followerState = cluster.getDivision(followerId);
    JavaUtils.attemptRepeatedly(() -> {
      Assert.assertTrue(followerState.getInfo().getLastAppliedIndex() >= leaderLastIndex);
      return null;
    }, 10, ONE_SECOND, "follower catchup", LOG);

    // make sure the restarted peer's log segments is correct
    final RaftServer.Division follower = cluster.restartServer(followerId, false);
    final RaftLog followerLog = follower.getRaftLog();
    final long followerLastIndex = followerLog.getLastEntryTermIndex().getIndex();
    Assert.assertTrue(followerLastIndex >= leaderLastIndex);

    final File followerOpenLogFile = getOpenLogFile(follower);
    final File leaderOpenLogFile = getOpenLogFile(cluster.getDivision(leaderId));

    // shutdown all servers
    for(RaftServer s : cluster.getServers()) {
      s.close();
    }

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
          final RaftClientReply reply = client.io().sendReadOnly(m);
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
        Assert.assertTrue(client.io().send(newMessage.get()).isSuccess());
      }
    }
  }

  static void assertTruncatedLog(RaftPeerId id, File openLogFile, long lastIndex, MiniRaftCluster cluster) throws Exception {
    // truncate log
    FileUtils.truncateFile(openLogFile, openLogFile.length() - 1);
    final RaftServer.Division server = cluster.restartServer(id, false);
    // the last index should be one less than before
    Assert.assertEquals(lastIndex - 1, server.getRaftLog().getLastEntryTermIndex().getIndex());
    server.getRaftServer().close();
  }

  static List<Path> getOpenLogFiles(RaftServer.Division server) throws Exception {
    return LogSegmentPath.getLogSegmentPaths(server.getRaftStorage()).stream()
        .filter(p -> p.getStartEnd().isOpen())
        .map(LogSegmentPath::getPath)
        .collect(Collectors.toList());
  }

  static File getOpenLogFile(RaftServer.Division server) throws Exception {
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
    for(RaftServer.Division impl : cluster.iterateDivisions()) {
      JavaUtils.attemptRepeatedly(() -> getOpenLogFile(impl), 10, TimeDuration.valueOf(100, TimeUnit.MILLISECONDS),
          impl.getId() + ": wait for log file creation", LOG);
    }

    // shutdown all servers
    for(RaftServer s : cluster.getServers()) {
      s.close();
    }

    for(RaftServer.Division impl : cluster.iterateDivisions()) {
      final File openLogFile = JavaUtils.attemptRepeatedly(() -> getOpenLogFile(impl),
          10, HUNDRED_MILLIS, impl.getId() + "-getOpenLogFile", LOG);
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
    final RaftServer.Division server = cluster.restartServer(id, false);
    server.getRaftServer().close();
  }

  @Test
  public void testRestartCommitIndex() throws Exception {
    runWithNewCluster(NUM_SERVERS, this::runTestRestartCommitIndex);
  }

  void runTestRestartCommitIndex(MiniRaftCluster cluster) throws Exception {
    final SimpleMessage[] messages = SimpleMessage.create(100);
    final List<CompletableFuture<Void>> futures = new ArrayList<>(messages.length);
    for(int i = 0; i < messages.length; i++) {
      final CompletableFuture<Void> f = new CompletableFuture<>();
      futures.add(f);

      final SimpleMessage m = messages[i];
      new Thread(() -> {
        try (final RaftClient client = cluster.createClient()) {
          Assert.assertTrue(client.io().send(m).isSuccess());
        } catch (IOException e) {
          throw new IllegalStateException("Failed to send " + m, e);
        }
        f.complete(null);
      }).start();
    }
    JavaUtils.allOf(futures).get();

    final List<RaftPeerId> ids = new ArrayList<>();
    final RaftServer.Division leader = cluster.getLeader();
    final RaftLog leaderLog = leader.getRaftLog();
    final RaftPeerId leaderId = leader.getId();
    ids.add(leaderId);

    RaftTestUtil.getStateMachineLogEntries(leaderLog);

    // check that the last metadata entry is written to the log
    JavaUtils.attempt(() -> assertLastLogEntry(leader), 20, HUNDRED_MILLIS, "leader last metadata entry", LOG);

    final long lastIndex = leaderLog.getLastEntryTermIndex().getIndex();
    LOG.info("{}: leader lastIndex={}", leaderId, lastIndex);
    final LogEntryProto lastEntry = leaderLog.get(lastIndex);
    LOG.info("{}: leader lastEntry entry[{}] = {}", leaderId, lastIndex, LogProtoUtils.toLogEntryString(lastEntry));
    final long loggedCommitIndex = lastEntry.getMetadataEntry().getCommitIndex();
    final LogEntryProto lastCommittedEntry = leaderLog.get(loggedCommitIndex);
    LOG.info("{}: leader lastCommittedEntry = entry[{}] = {}",
        leaderId, loggedCommitIndex, LogProtoUtils.toLogEntryString(lastCommittedEntry));

    final SimpleStateMachine4Testing leaderStateMachine = SimpleStateMachine4Testing.get(leader);
    final TermIndex lastAppliedTermIndex = leaderStateMachine.getLastAppliedTermIndex();
    LOG.info("{}: leader lastAppliedTermIndex = {}", leaderId, lastAppliedTermIndex);

    // check follower logs
    for(RaftServer.Division s : cluster.iterateDivisions()) {
      if (!s.getId().equals(leaderId)) {
        ids.add(s.getId());
        JavaUtils.attempt(() -> RaftTestUtil.assertSameLog(leaderLog, s.getRaftLog()),
            10, HUNDRED_MILLIS, "assertRaftLog-" + s.getId(), LOG);
      }
    }

    // take snapshot and truncate last (metadata) entry
    leaderStateMachine.takeSnapshot();
    leaderLog.truncate(lastIndex);

    // kill all servers
    ids.forEach(cluster::killServer);

    // Restart and kill servers one by one so that they won't talk to each other.
    for(RaftPeerId id : ids) {
      cluster.restartServer(id, false);
      final RaftServer.Division server = cluster.getDivision(id);
      final RaftLog raftLog = server.getRaftLog();
      JavaUtils.attemptRepeatedly(() -> {
        Assert.assertTrue(raftLog.getLastCommittedIndex() >= loggedCommitIndex);
        return null;
      }, 10, HUNDRED_MILLIS, id + "(commitIndex >= loggedCommitIndex)", LOG);
      JavaUtils.attemptRepeatedly(() -> {
        Assert.assertTrue(server.getInfo().getLastAppliedIndex() >= loggedCommitIndex);
        return null;
      }, 10, HUNDRED_MILLIS, id + "(lastAppliedIndex >= loggedCommitIndex)", LOG);
      LOG.info("{}: commitIndex={}, lastAppliedIndex={}",
          id, raftLog.getLastCommittedIndex(), server.getInfo().getLastAppliedIndex());
      cluster.killServer(id);
    }
  }

  static void assertLastLogEntry(RaftServer.Division server) throws RaftLogIOException {
    final RaftLog raftLog = server.getRaftLog();
    final long lastIndex = raftLog.getLastEntryTermIndex().getIndex();
    final LogEntryProto lastEntry = raftLog.get(lastIndex);
    Assert.assertTrue(lastEntry.hasMetadataEntry());

    final long loggedCommitIndex = lastEntry.getMetadataEntry().getCommitIndex();
    final LogEntryProto lastCommittedEntry = raftLog.get(loggedCommitIndex);
    Assert.assertTrue(lastCommittedEntry.hasStateMachineLogEntry());

    final SimpleStateMachine4Testing leaderStateMachine = SimpleStateMachine4Testing.get(server);
    final TermIndex lastAppliedTermIndex = leaderStateMachine.getLastAppliedTermIndex();
    Assert.assertEquals(lastCommittedEntry.getTerm(), lastAppliedTermIndex.getTerm());
    Assert.assertTrue(lastCommittedEntry.getIndex() <= lastAppliedTermIndex.getIndex());
  }

  @Test
  public void testRestartWithCorruptedLogEntryWithWarnAndReturn() throws Exception {
    final RaftProperties p = getProperties();
    final Log.CorruptionPolicy policy = Log.corruptionPolicy(p);
    Log.setCorruptionPolicy(p, Log.CorruptionPolicy.WARN_AND_RETURN);

    runWithNewCluster(1, this::runTestRestartWithCorruptedLogEntry);

    Log.setCorruptionPolicy(p, policy);
  }

  @Test
  public void testRestartWithCorruptedLogEntryWithException() throws Exception {
    final RaftProperties p = getProperties();
    final Log.CorruptionPolicy policy = Log.corruptionPolicy(p);
    Log.setCorruptionPolicy(p, Log.CorruptionPolicy.EXCEPTION);

    testFailureCase("restart-fail-ChecksumException",
        () -> runWithNewCluster(1, this::runTestRestartWithCorruptedLogEntry),
        CompletionException.class, ChecksumException.class);

    Log.setCorruptionPolicy(p, policy);
  }

  private void runTestRestartWithCorruptedLogEntry(CLUSTER cluster) throws Exception {
    // this is the only server
    final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster);
    final RaftPeerId id = leader.getId();

    // send a few messages
    final SimpleMessage[] messages = SimpleMessage.create(10);
    final SimpleMessage lastMessage = messages[messages.length - 1];
    try (final RaftClient client = cluster.createClient()) {
      for (SimpleMessage m : messages) {
        Assert.assertTrue(client.io().send(m).isSuccess());
      }

      // assert that the last message exists
      Assert.assertTrue(client.io().sendReadOnly(lastMessage).isSuccess());
    }

    final RaftLog log = leader.getRaftLog();
    final long size = TestSegmentedRaftLog.getOpenSegmentSize(log);
    leader.getRaftServer().close();

    // corrupt the log
    final File openLogFile = JavaUtils.attemptRepeatedly(() -> getOpenLogFile(leader),
        10, HUNDRED_MILLIS, id + "-getOpenLogFile", LOG);
    try(final RandomAccessFile raf = new RandomAccessFile(openLogFile, "rw")) {
      final long mid = size / 2;
      raf.seek(mid);
      for (long i = mid; i < size; i++) {
        raf.write(0);
      }
    }

    // after the log is corrupted and the server is restarted, the last entry should no longer exist.
    cluster.restartServer(id, false);
    testFailureCase("last-entry-not-found", () -> {
      try (final RaftClient client = cluster.createClient()) {
        client.io().sendReadOnly(lastMessage);
      }
    }, StateMachineException.class, IndexOutOfBoundsException.class);
  }
}
