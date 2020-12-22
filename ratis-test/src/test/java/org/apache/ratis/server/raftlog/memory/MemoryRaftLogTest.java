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
package org.apache.ratis.server.raftlog.memory;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.CompletableFuture;
import org.apache.log4j.Level;
import org.apache.ratis.BaseTest;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.LogEntryHeader;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.statemachine.SimpleStateMachine4Testing;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.Log4jUtils;
import org.junit.Test;

public class MemoryRaftLogTest extends BaseTest {
  static {
    Log4jUtils.setLogLevel(MemoryRaftLog.LOG, Level.DEBUG);
  }

  @Test
  public void testEntryDoNotPerformTruncation() throws Exception {
    final RaftProperties prop = new RaftProperties();
    prop.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        SimpleStateMachine4Testing.class, StateMachine.class);
    final RaftPeerId peerId = RaftPeerId.valueOf("s0");
    final RaftGroupId groupId = RaftGroupId.randomId();
    final RaftGroupMemberId memberId = RaftGroupMemberId.valueOf(peerId, groupId);

    MemoryRaftLog raftLog = new MemoryRaftLog(memberId, () -> -1, prop);
    raftLog.open(RaftLog.INVALID_LOG_INDEX, null);
    LogEntryProto[] entries1 = new LogEntryProto[2];
    entries1[0] = LogEntryProto.newBuilder().setIndex(0).setTerm(0).build();
    entries1[1] = LogEntryProto.newBuilder().setIndex(1).setTerm(0).build();
    raftLog.append(entries1).forEach(CompletableFuture::join);

    LogEntryProto[] entries2 = new LogEntryProto[1];
    entries2[0] = LogEntryProto.newBuilder().setIndex(0).setTerm(0).build();
    raftLog.append(entries2).forEach(CompletableFuture::join);

    final LogEntryHeader[] termIndices = raftLog.getEntries(0, 10);
    assertEquals(2, termIndices.length);
    for (int i = 0; i < 2; i++) {
      assertEquals(entries1[i].getIndex(), termIndices[i].getIndex());
      assertEquals(entries1[i].getTerm(), termIndices[i].getTerm());
    }
  }

  @Test
  public void testEntryPerformTruncation() throws Exception {
    final RaftProperties prop = new RaftProperties();
    prop.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        SimpleStateMachine4Testing.class, StateMachine.class);
    final RaftPeerId peerId = RaftPeerId.valueOf("s0");
    final RaftGroupId groupId = RaftGroupId.randomId();
    final RaftGroupMemberId memberId = RaftGroupMemberId.valueOf(peerId, groupId);

    MemoryRaftLog raftLog = new MemoryRaftLog(memberId, () -> -1, prop);
    raftLog.open(RaftLog.INVALID_LOG_INDEX, null);
    LogEntryProto[] entries1 = new LogEntryProto[2];
    entries1[0] = LogEntryProto.newBuilder().setIndex(0).setTerm(0).build();
    entries1[1] = LogEntryProto.newBuilder().setIndex(1).setTerm(0).build();
    raftLog.append(entries1).forEach(CompletableFuture::join);

    LogEntryProto[] entries2 = new LogEntryProto[1];
    entries2[0] = LogEntryProto.newBuilder().setIndex(0).setTerm(2).build();
    raftLog.append(entries2).forEach(CompletableFuture::join);

    final LogEntryHeader[] termIndices = raftLog.getEntries(0, 10);
    assertEquals(1, termIndices.length);
    assertEquals(entries2[0].getIndex(), termIndices[0].getIndex());
    assertEquals(entries2[0].getTerm(), termIndices[0].getTerm());
  }
}
