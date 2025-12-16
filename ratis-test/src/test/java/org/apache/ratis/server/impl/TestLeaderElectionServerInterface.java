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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertFalse;
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
      private volatile boolean isAlive = true;

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
        return new LeaderElection.ConfAndTerm(conf, getCurrentTerm());
      }

      @Override
      public RequestVoteReplyProto requestVote(RequestVoteRequestProto r) {
        final RaftPeerId voterPeerId = RaftPeerId.valueOf(r.getServerRequest().getReplyId());
        final RaftGroupMemberId voter = RaftGroupMemberId.valueOf(voterPeerId, group.getGroupId());
        final TermIndex lastEntry = lastEntries.get(voterPeerId);
        final long term = (lastEntry != null? lastEntry : TermIndex.INITIAL_VALUE).getTerm();

        // voter replies to candidate
        return ServerProtoUtils.toRequestVoteReplyProto(getId(), voter, true, term, false, lastEntry);
      }

      @Override
      public void changeToLeader() {
        assertTrue(expectToPass);
        isAlive = false;
      }

      @Override
      public void rejected(long term, LeaderElection.ResultAndTerm result) {
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