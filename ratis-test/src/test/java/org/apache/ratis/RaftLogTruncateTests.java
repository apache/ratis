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

import org.apache.ratis.RaftTestUtil.SimpleMessage;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.client.impl.OrderedAsync;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.server.impl.RaftServerTestUtil;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.raftlog.segmented.SegmentedRaftLog;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.SimpleStateMachine4Testing;
import org.apache.ratis.util.MemoizedSupplier;
import org.apache.ratis.util.Slf4jUtils;
import org.apache.ratis.util.TimeDuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.event.Level;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.ratis.RaftTestUtil.waitForLeader;

public abstract class RaftLogTruncateTests<CLUSTER extends MiniRaftCluster> extends BaseTest
    implements MiniRaftCluster.Factory.Get<CLUSTER> {
  public static final int NUM_SERVERS = 5;
  final TimeDuration MIN_TIMEOUT = TimeDuration.valueOf(3, TimeUnit.SECONDS);

  static SimpleMessage[] arraycopy(SimpleMessage[] src1, SimpleMessage[] src2) {
    final SimpleMessage[] dst = new SimpleMessage[src1.length + src2.length];
    System.arraycopy(src1, 0, dst, 0, src1.length);
    System.arraycopy(src2, 0, dst, src1.length, src2.length);
    return dst;
  }

  {
    Slf4jUtils.setLogLevel(RaftServerTestUtil.getTransactionContextLog(), Level.TRACE);

    Slf4jUtils.setLogLevel(OrderedAsync.LOG, Level.ERROR);
    Slf4jUtils.setLogLevel(RaftServerConfigKeys.LOG, Level.ERROR);
    Slf4jUtils.setLogLevel(RaftClientConfigKeys.LOG, Level.ERROR);
    Slf4jUtils.setLogLevel(RaftClientConfigKeys.LOG, Level.ERROR);

    final RaftProperties p = getProperties();
    p.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY, SimpleStateMachine4Testing.class, StateMachine.class);

    // set a long rpc timeout so, when the leader does not have the majority, it won't step down fast.
    RaftServerConfigKeys.Rpc.setTimeoutMin(p, MIN_TIMEOUT);
    RaftServerConfigKeys.Rpc.setTimeoutMax(p, MIN_TIMEOUT.multiply(2));
    RaftServerConfigKeys.Rpc.setFirstElectionTimeoutMin(p, TimeDuration.ONE_SECOND);
    RaftServerConfigKeys.Rpc.setFirstElectionTimeoutMax(p, TimeDuration.ONE_SECOND.multiply(2));
  }

  @Override
  public int getGlobalTimeoutSeconds() {
    return 200;
  }

  @Test
  public void testLogTruncate() throws Exception {
    runWithNewCluster(NUM_SERVERS, this::runTestLogTruncate);
  }

  void runTestLogTruncate(MiniRaftCluster cluster) throws Exception {
    final RaftServer.Division oldLeader = waitForLeader(cluster);
    final List<RaftServer.Division> oldFollowers = cluster.getFollowers();
    final List<RaftPeerId> killedPeers = new ArrayList<>();
    final List<RaftPeerId> remainingPeers = new ArrayList<>();

    final int majorityIndex = NUM_SERVERS / 2 + 1;
    Assertions.assertEquals(NUM_SERVERS - 1, oldFollowers.size());
    Assertions.assertTrue(majorityIndex < oldFollowers.size());

    for (int i = 0; i < majorityIndex; i++) {
      killedPeers.add(oldFollowers.get(i).getId());
    }
    remainingPeers.add(oldLeader.getId());
    for (int i = majorityIndex; i < oldFollowers.size(); i++) {
      remainingPeers.add(oldFollowers.get(i).getId());
    }

    try {
      runTestLogTruncate(cluster, oldLeader, killedPeers, remainingPeers);
    } catch (Throwable e) {
      LOG.info("killedPeers   : {}", killedPeers);
      LOG.info("remainingPeers: {}", remainingPeers);
      throw e;
    }
  }

  void runTestLogTruncate(MiniRaftCluster cluster, RaftServer.Division oldLeader,
      List<RaftPeerId> killedPeers, List<RaftPeerId> remainingPeers) throws Exception {
    final List<Throwable> exceptions = Collections.synchronizedList(new ArrayList<>());
    final long oldLeaderTerm = oldLeader.getInfo().getCurrentTerm();
    LOG.info("oldLeader: {}, term={}", oldLeader.getId(), oldLeaderTerm);
    LOG.info("killedPeers   : {}", killedPeers);
    LOG.info("remainingPeers: {}", remainingPeers);

    final SimpleMessage[] firstBatch = SimpleMessage.create(5, "first");
    final SimpleMessage[] secondBatch = SimpleMessage.create(4, "second");

    for (RaftPeer peer : cluster.getGroup().getPeers()) {
      assertEmptyTransactionContextMap(cluster.getDivision(peer.getId()));
    }

    try (final RaftClient client = cluster.createClient(oldLeader.getId())) {
      // send some messages
      for (SimpleMessage batch : firstBatch) {
        final RaftClientReply reply = client.io().send(batch);
        Assertions.assertTrue(reply.isSuccess());
      }
      for (RaftPeer peer : cluster.getGroup().getPeers()) {
        final RaftServer.Division division = cluster.getDivision(peer.getId());
        assertLogEntries(division, oldLeaderTerm, firstBatch);
        assertEmptyTransactionContextMap(division);
      }

      // kill a majority of followers
      LOG.info("Before killServer {}: {}", killedPeers, cluster.printServers());
      for (RaftPeerId f : killedPeers) {
        cluster.killServer(f);
      }
      LOG.info("After killServer {}: {}", killedPeers, cluster.printServers());

      // send more messages, but they won't be committed due to not enough followers
      final SimpleMessage[] messagesToBeTruncated = SimpleMessage.create(3, "messagesToBeTruncated");
      final AtomicBoolean done = new AtomicBoolean();
      for (SimpleMessage message : messagesToBeTruncated) {
        client.async().send(message).whenComplete((r, e) -> {
          if (!done.get()) {
            exceptions.add(new IllegalStateException(message + " is completed: reply=" + r, e));
          }
        });
      }

      // check log messages
      final SimpleMessage[] expectedMessages = arraycopy(firstBatch, messagesToBeTruncated);
      for (RaftPeerId f : remainingPeers) {
        final RaftServer.Division division = cluster.getDivision(f);
        assertLogEntries(division, oldLeaderTerm, expectedMessages);
        if (!division.getId().equals(oldLeader.getId())) {
          assertEntriesInTransactionContextMap(division, messagesToBeTruncated, firstBatch);
        }
      }
      done.set(true);
      LOG.info("done");
    }

    // kill the old leader
    LOG.info("Before killServer {}: {}", oldLeader.getId(), cluster.printServers());
    cluster.killServer(oldLeader.getId());
    LOG.info("After killServer {}: {}", remainingPeers, cluster.printServers());

    // restart the earlier followers
    for (RaftPeerId f : killedPeers) {
      cluster.restartServer(f, false);
    }

    // The new leader should be one of the earlier followers
    final RaftServer.Division newLeader = waitForLeader(cluster);
    LOG.info("After restartServer {}: {}", killedPeers, cluster.printServers());
    final long newLeaderTerm = newLeader.getInfo().getCurrentTerm();

    final SegmentedRaftLog newLeaderLog = (SegmentedRaftLog) newLeader.getRaftLog();
    LOG.info("newLeader: {}, term {}, last={}", newLeader.getId(), newLeaderTerm,
        newLeaderLog.getLastEntryTermIndex());
    Assertions.assertTrue(killedPeers.contains(newLeader.getId()));

    // restart the old leader
    cluster.restartServer(oldLeader.getId(), false);

    // check RaftLog truncate
    for (RaftPeerId f : remainingPeers) {
      assertLogEntries(cluster.getDivision(f), oldLeaderTerm, firstBatch);
    }

    try (final RaftClient client = cluster.createClient(newLeader.getId())) {
      // send more messages
      for (SimpleMessage batch : secondBatch) {
        final RaftClientReply reply = client.io().send(batch);
        Assertions.assertTrue(reply.isSuccess());
      }
    }

    // check log messages -- it should be truncated and then append the new messages
    final SimpleMessage[] expectedMessages = arraycopy(firstBatch, secondBatch);
    for (RaftPeer peer : cluster.getGroup().getPeers()) {
      final RaftServer.Division division = cluster.getDivision(peer.getId());
      assertLogEntries(division, oldLeaderTerm, expectedMessages);
      assertEmptyTransactionContextMap(division);
    }

    if (!exceptions.isEmpty()) {
      LOG.info("{} exceptions", exceptions.size());
      for(int i = 0 ; i < exceptions.size(); i++) {
        LOG.info("exception {})", i, exceptions.get(i));
      }
      Assertions.fail();
    }
  }

  static void assertEmptyTransactionContextMap(RaftServer.Division division) {
    Assertions.assertTrue(RaftServerTestUtil.getTransactionContextMap(division).isEmpty(),
        () -> division.getId() + " TransactionContextMap is non-empty");
  }

  static void assertEntriesInTransactionContextMap(RaftServer.Division division,
      SimpleMessage[] existing, SimpleMessage[] nonExisting) {
    final RaftLog log = division.getRaftLog();
    assertEntriesInTransactionContextMap(division,
        RaftTestUtil.getStateMachineLogEntries(log, existing).values(),
        RaftTestUtil.getStateMachineLogEntries(log, nonExisting).values());
  }

  static void assertEntriesInTransactionContextMap(RaftServer.Division division,
      Collection<LogEntryProto> existing, Collection<LogEntryProto> nonExisting) {
    final Map<TermIndex, MemoizedSupplier<TransactionContext>> map
        = RaftServerTestUtil.getTransactionContextMap(division);
    for(LogEntryProto e : existing) {
      final TermIndex termIndex = TermIndex.valueOf(e);
      Assertions.assertTrue(map.containsKey(termIndex),
          () -> termIndex + " not found in " + division.getId());
    }
    for(LogEntryProto e : nonExisting) {
      final TermIndex termIndex = TermIndex.valueOf(e);
      Assertions.assertFalse(map.containsKey(termIndex),
          () -> termIndex + " found in " + division.getId());
    }
  }

  private void assertLogEntries(RaftServer.Division server, long term, SimpleMessage[] expectedMessages)
      throws Exception {
    RaftTestUtil.assertLogEntries(server, term, expectedMessages, 30, LOG);
  }
}
