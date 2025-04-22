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

import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.GroupMismatchException;
import org.apache.ratis.server.RaftConfiguration;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.IOUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.TimeDuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/** Server utilities for internal use. */
public final class ServerImplUtils {
  /** The consecutive indices within the same term. */
  static class ConsecutiveIndices {
    /** Convert the given entries to a list of {@link ConsecutiveIndices} */
    static List<ConsecutiveIndices> convert(List<LogEntryProto> entries) {
      if (entries == null || entries.isEmpty()) {
        return Collections.emptyList();
      }

      List<ConsecutiveIndices> indices = null;

      LogEntryProto previous = entries.get(0);
      long startIndex = previous.getIndex();
      int count = 1;

      for (int i = 1; i < entries.size(); i++) {
        final LogEntryProto current = entries.get(i);
        // validate if the indices are consecutive
        Preconditions.assertSame(previous.getIndex() + 1, current.getIndex(), "index");

        if (current.getTerm() == previous.getTerm()) {
          count++;
        } else {
          // validate if the terms are increasing
          Preconditions.assertTrue(previous.getTerm() < current.getTerm(), "term");
          if (indices == null) {
            indices = new ArrayList<>();
          }
          indices.add(new ConsecutiveIndices(previous.getTerm(), startIndex, count));

          startIndex = current.getIndex();
          count = 1;
        }
        previous = current;
      }

      final ConsecutiveIndices last = new ConsecutiveIndices(previous.getTerm(), startIndex, count);
      if (indices == null) {
        return Collections.singletonList(last);
      } else {
        indices.add(last);
        return indices;
      }
    }

    private final long term;
    private final long startIndex;
    private final int count;

    ConsecutiveIndices(long term, long startIndex, int count) {
      Preconditions.assertTrue(count > 0, () -> "count = " + count + " <= 0 ");
      this.term = term;
      this.startIndex = startIndex;
      this.count = count;
    }

    long getNextIndex() {
      return startIndex + count;
    }

    Long getTerm(long index) {
      final long diff = index - startIndex;
      return diff < 0 || diff >= count ? null: term;
    }
  }

  /** A data structure to support the {@link #contains(TermIndex)} method. */
  static class NavigableIndices {
    private final NavigableMap<Long, ConsecutiveIndices> map = new TreeMap<>();

    boolean contains(TermIndex ti) {
      final Long term = getTerm(ti.getIndex());
      return term != null && term == ti.getTerm();
    }

    synchronized Long getTerm(long index) {
      if (map.isEmpty()) {
        return null;
      }

      final Map.Entry<Long, ConsecutiveIndices> floorEntry = map.floorEntry(index);
      if (floorEntry == null) {
        return null;
      }
      return floorEntry.getValue().getTerm(index);
    }

    synchronized boolean append(List<ConsecutiveIndices> entriesTermIndices) {
      for(int i = 0; i < entriesTermIndices.size(); i++) {
        final ConsecutiveIndices indices = entriesTermIndices.get(i);
        final ConsecutiveIndices previous = map.put(indices.startIndex, indices);
        if (previous != null) {
          // index already exists, revert this append
          map.put(previous.startIndex, previous);
          for(int j = 0; j < i; j++) {
            map.remove(entriesTermIndices.get(j).startIndex);
          }
          return false;
        }
      }
      return true;
    }

    synchronized void removeExisting(List<ConsecutiveIndices> entriesTermIndices) {
      for(ConsecutiveIndices indices : entriesTermIndices) {
        final ConsecutiveIndices removed = map.remove(indices.startIndex);
        Preconditions.assertSame(indices, removed, "removed");
      }
    }
  }

  private ServerImplUtils() {
    //Never constructed
  }

  /** Create a {@link RaftServerProxy}. */
  public static RaftServerProxy newRaftServer(
      RaftPeerId id, RaftGroup group, RaftStorage.StartupOption option, StateMachine.Registry stateMachineRegistry,
      ThreadGroup threadGroup, RaftProperties properties, Parameters parameters) throws IOException {
    RaftServer.LOG.debug("newRaftServer: {}, {}", id, group);
    if (group != null && !group.getPeers().isEmpty()) {
      Preconditions.assertNotNull(id, "RaftPeerId %s is not in RaftGroup %s", id, group);
      Preconditions.assertNotNull(group.getPeer(id), "RaftPeerId %s is not in RaftGroup %s", id, group);
    }
    final RaftServerProxy proxy = newRaftServer(id, stateMachineRegistry, threadGroup, properties, parameters);
    proxy.initGroups(group, option);
    return proxy;
  }

  private static RaftServerProxy newRaftServer(
      RaftPeerId id, StateMachine.Registry stateMachineRegistry, ThreadGroup threadGroup, RaftProperties properties,
      Parameters parameters) throws IOException {
    final TimeDuration sleepTime = TimeDuration.valueOf(500, TimeUnit.MILLISECONDS);
    final RaftServerProxy proxy;
    try {
      // attempt multiple times to avoid temporary bind exception
      proxy = JavaUtils.attemptRepeatedly(
          () -> new RaftServerProxy(id, stateMachineRegistry, properties, parameters, threadGroup),
          5, sleepTime, "new RaftServerProxy", RaftServer.LOG);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw IOUtils.toInterruptedIOException(
          "Interrupted when creating RaftServer " + id, e);
    }
    return proxy;
  }

  public static RaftConfiguration newRaftConfiguration(List<RaftPeer> conf, List<RaftPeer> listener,
      long index, List<RaftPeer> oldConf, List<RaftPeer> oldListener) {
    final RaftConfigurationImpl.Builder b = RaftConfigurationImpl.newBuilder()
        .setConf(conf, listener)
        .setLogEntryIndex(index);
    if (!oldConf.isEmpty() || !oldListener.isEmpty()) {
      b.setOldConf(oldConf, oldListener);
    }
    return b.build();
  }

  static long effectiveCommitIndex(long leaderCommitIndex, TermIndex followerPrevious, int numAppendEntries) {
    final long previous = followerPrevious != null? followerPrevious.getIndex() : RaftLog.LEAST_VALID_LOG_INDEX;
    return Math.min(leaderCommitIndex, previous + numAppendEntries);
  }

  static void assertGroup(RaftGroupMemberId serverMemberId, RaftClientRequest request) throws GroupMismatchException {
    assertGroup(serverMemberId, request.getRequestorId(), request.getRaftGroupId());
  }

  static void assertGroup(RaftGroupMemberId localMemberId, Object remoteId, RaftGroupId remoteGroupId)
      throws GroupMismatchException {
    final RaftGroupId localGroupId = localMemberId.getGroupId();
    if (!localGroupId.equals(remoteGroupId)) {
      throw new GroupMismatchException(localMemberId
          + ": The group (" + remoteGroupId + ") of remote " + remoteId
          + " does not match the group (" + localGroupId + ") of local " + localMemberId.getPeerId());
    }
  }

  static void assertEntries(AppendEntriesRequestProto proto, TermIndex previous, ServerState state) {
    final List<LogEntryProto> entries = proto.getEntriesList();
    if (entries != null && !entries.isEmpty()) {
      final long index0 = entries.get(0).getIndex();
      // Check if next entry's index is 1 greater than the snapshotIndex. If yes, then
      // we do not have to check for the existence of previous.
      if (index0 != state.getSnapshotIndex() + 1) {
        final long expected = previous == null || previous.getTerm() == 0 ? 0 : previous.getIndex() + 1;
        Preconditions.assertTrue(index0 == expected,
            "Unexpected Index: previous is %s but entries[%s].getIndex() == %s != %s",
            previous, 0, index0, expected);
      }

      final long leaderTerm = proto.getLeaderTerm();
      for (int i = 0; i < entries.size(); i++) {
        final LogEntryProto entry = entries.get(i);
        final long entryTerm = entry.getTerm();
        Preconditions.assertTrue(entryTerm <= leaderTerm ,
            "Unexpected Term: entries[%s].getTerm() == %s > leaderTerm == %s",
            i, entryTerm, leaderTerm);

        final long indexI = entry.getIndex();
        final long expected = index0 + i;
        Preconditions.assertTrue(indexI == expected,
            "Unexpected Index: entries[0].getIndex() == %s but entries[%s].getIndex() == %s != %s",
            index0, i, indexI, expected);
      }
    }
  }
}
