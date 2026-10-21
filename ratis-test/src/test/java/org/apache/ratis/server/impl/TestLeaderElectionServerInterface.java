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
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.metrics.Timekeeper;
import org.apache.ratis.proto.RaftProtos.RequestVoteReplyProto;
import org.apache.ratis.proto.RaftProtos.RequestVoteRequestProto;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.util.TimeDuration;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestLeaderElectionServerInterface extends BaseTest {
  private final List<RaftPeer> peers = IntStream.range(0, 3).boxed()
      .map(i -> RaftPeer.newBuilder().setId("s" + i).build())
      .collect(Collectors.toList());
  private final RaftGroup group = RaftGroup.valueOf(RaftGroupId.randomId(), peers);
  private final RaftConfigurationImpl conf = RaftConfigurationImpl.newBuilder().setLogEntryIndex(0).setConf(peers).build();
  private final ThreadGroup threadGroup = new ThreadGroup("ServerInterface");

  private final RaftGroupMemberId candidate = RaftGroupMemberId.valueOf(peers.get(0).getId(), group.getGroupId());

  LeaderElection.ServerInterface newServerInterface(boolean expectToPass,
      Map<RaftPeerId, TermIndex> lastEntries) {
    return new LeaderElection.ServerInterface() {
      // Keep the monitor distinct from the adapter to detect synchronization on the wrong object.
      private final Object lock = new Object();
      private volatile boolean isAlive = true;

      @Override
      public Object getLock() {
        return lock;
      }

      @Override
      public RaftGroupMemberId getMemberId() {
        return candidate;
      }

      @Override
      public boolean isAlive() {
        return isAlive;
      }

      @Override
      public boolean isCandidate() {
        return true;
      }

      @Override
      public long getCurrentTerm() {
        final TermIndex lastEntry = getLastEntry();
        return lastEntry != null? lastEntry.getTerm() : TermIndex.INITIAL_VALUE.getTerm();
      }

      @Override
      public long getLastCommittedIndex() {
        final TermIndex lastEntry = getLastEntry();
        return lastEntry != null? lastEntry.getIndex() : TermIndex.INITIAL_VALUE.getIndex();
      }

      @Override
      public TermIndex getLastEntry() {
        return lastEntries.get(getId());
      }

      @Override
      public boolean isPreVoteEnabled() {
        return false;
      }

      @Override
      public LeaderElection.ConfAndTerm initElection(LeaderElection.Phase phase) {
        assertTrue(Thread.holdsLock(lock), "Election initialization must hold the shared lock");
        return new LeaderElection.ConfAndTerm(conf, getCurrentTerm());
      }

      @Override
      public RequestVoteReplyProto requestVote(RequestVoteRequestProto r) {
        final RaftPeerId voterPeerId = RaftPeerId.valueOf(r.getServerRequest().getReplyId());
        final RaftGroupMemberId voter = RaftGroupMemberId.valueOf(voterPeerId, group.getGroupId());
        final TermIndex lastEntry = lastEntries.get(voterPeerId);
        final long term = (lastEntry != null? lastEntry : TermIndex.INITIAL_VALUE).getTerm();

        // voter replies to candidate
        return ServerProtoUtils.toRequestVoteReplyProto(
            getId(), voter, true, term, false, lastEntry, r.getServerRequest().getCallId());
      }

      @Override
      public void changeToLeader() {
        assertTrue(expectToPass);
        isAlive = false;
      }

      @Override
      public void rejected(long term, LeaderElection.ResultAndTerm result) {
        assertTrue(Thread.holdsLock(lock), "Election result handling must hold the shared lock");
        assertFalse(expectToPass);
        isAlive = false;
      }

      @Override
      public void shutdown() {
        fail();
      }

      @Override
      public Timekeeper getLeaderElectionTimer() {
        final long start = System.nanoTime();
        final Timekeeper.Context context = () -> System.nanoTime() - start;
        return () -> context;
      }

      @Override
      public void onNewLeaderElectionCompletion() {
        // no op
      }

      @Override
      public TimeDuration getRandomElectionTimeout() {
        final int millis = 100 + ThreadLocalRandom.current().nextInt(100);
        return TimeDuration.valueOf(millis, TimeUnit.MILLISECONDS);
      }

      @Override
      public ThreadGroup getThreadGroup() {
        return threadGroup;
      }
    };
  }

  @Test
  public void testAdapterUsesServerLock() {
    final RaftServerImpl server = Mockito.mock(RaftServerImpl.class);
    final RaftServerProxy proxy = Mockito.mock(RaftServerProxy.class);
    Mockito.when(server.getRaftServer()).thenReturn(proxy);
    Mockito.when(proxy.getProperties()).thenReturn(new RaftProperties());
    assertSame(server, LeaderElection.ServerInterface.get(server).getLock());
  }

  @Test
  public void testElectionSharesServerLock() {
    final Map<RaftPeerId, TermIndex> lastEntries = new HashMap<>();
    lastEntries.put(peers.get(0).getId(), TermIndex.valueOf(1, 0));
    // A higher-term reply exercises the result-handling path that persists metadata when stepping down.
    lastEntries.put(peers.get(1).getId(), TermIndex.valueOf(2, 0));
    lastEntries.put(peers.get(2).getId(), TermIndex.valueOf(2, 0));
    // Normal elections initialize in askForVotes; forced elections initialize in newInstance.
    for (boolean force : new boolean[] {false, true}) {
      final LeaderElection.ServerInterface server = newServerInterface(false, lastEntries);
      LeaderElection.newInstance(server, force).startInForeground();
      assertFalse(server.isAlive(), "The election must stop after handling the higher term");
    }
  }

  @Test
  public void testVoterWithEmptyLog() {
    // all the candidate and the voters have an empty log
    // expect to pass: empty-log-candidate will accept votes from empty-log-voters
    runTestVoterWithEmptyLog(true);

    // candidate: non-empty commit
    // voter 1  : empty log
    // voter 2  : empty log
    // expect to fail: non-empty-commit-candidate will NOT accept votes from empty-log-voters
    final TermIndex candidateLastEntry = TermIndex.valueOf(2, 9);
    runTestVoterWithEmptyLog(false, candidateLastEntry);

    // candidate: non-empty commit
    // voter 1  : non-empty log
    // voter 2  : empty log
    // expect to pass: non-empty-commit-candidate will accept votes from non-empty-log-voters
    final TermIndex voterLastEntry = TermIndex.valueOf(2, 7);
    runTestVoterWithEmptyLog(true, candidateLastEntry, voterLastEntry);

    // candidate: non-empty log
    // voter 1  : older version
    // voter 2  : empty log
    // expect to pass: non-empty-commit-candidate will accept votes from older-version-voters
    runTestVoterWithEmptyLog(true, candidateLastEntry, TermIndex.PROTO_DEFAULT);
  }

  void runTestVoterWithEmptyLog(boolean expectToPass, TermIndex... lastEntries) {
    LOG.info("expectToPass? {}, lastEntries={}",
        expectToPass, lastEntries);
    final Map<RaftPeerId, TermIndex> map = new HashMap<>();
    for(int i = 0; i < lastEntries.length; i++) {
      map.put(peers.get(i).getId(), lastEntries[i]);
    }
    final LeaderElection election = LeaderElection.newInstance(newServerInterface(expectToPass, map), false);
    election.startInForeground();
  }

}
