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
import org.apache.ratis.server.impl.ServerState;
import org.apache.ratis.server.storage.RaftLog;
import org.apache.ratis.server.storage.RaftStorageDirectory.LogPathAndIndex;
import org.apache.ratis.statemachine.SimpleStateMachine4Testing;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.LogUtils;
import org.apache.ratis.util.SizeInBytes;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
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
    LogUtils.setLogLevel(RaftServerImpl.LOG, Level.DEBUG);
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
    try(final MiniRaftCluster cluster = newCluster(NUM_SERVERS)) {
      runTestRestartFollower(cluster, LOG);
    }
  }

  static void runTestRestartFollower(MiniRaftCluster cluster, Logger LOG) throws Exception {
    cluster.start();
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

  static File getOpenLogFile(RaftServerImpl server) throws Exception {
    final List<Path> openLogs = server.getState().getStorage().getStorageDir().getLogSegmentFiles().stream()
        .filter(LogPathAndIndex::isOpen)
        .map(LogPathAndIndex::getPath)
        .collect(Collectors.toList());
    Assert.assertEquals(1, openLogs.size());
    return openLogs.get(0).toFile();
  }
}
