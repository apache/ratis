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
package org.apache.ratis.client;


import org.apache.ratis.BaseTest;
import org.apache.ratis.client.impl.ClientProtoUtils;
import org.apache.ratis.proto.RaftProtos.CommitInfoProto;
import org.apache.ratis.proto.RaftProtos.GroupInfoReplyProto;
import org.apache.ratis.proto.RaftProtos.LeaderInfoProto;
import org.apache.ratis.proto.RaftProtos.LogInfoProto;
import org.apache.ratis.proto.RaftProtos.RaftClientRequestProto;
import org.apache.ratis.proto.RaftProtos.RaftConfigurationProto;
import org.apache.ratis.proto.RaftProtos.RaftPeerProto;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.proto.RaftProtos.RoleInfoProto;
import org.apache.ratis.proto.RaftProtos.TermIndexProto;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.GroupInfoReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.Timestamp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

/** Test {@link ClientProtoUtils}. */
public class TestClientProtoUtils extends BaseTest {
  @Test
  public void testToRaftClientRequestProto() throws Exception {
    for(int i = 1; i < 32; i <<= 2) {
      final SizeInBytes messageSize = SizeInBytes.valueOf(i + "MB");
      runTestToRaftClientRequestProto(100, messageSize);
    }
  }

  void runTestToRaftClientRequestProto(int n, SizeInBytes messageSize)
      throws Exception {
    final ClientId clientId = ClientId.randomId();
    final RaftPeerId leaderId = RaftPeerId.valueOf("s0");
    final RaftGroupId groupId = RaftGroupId.randomId();


    TimeDuration toProto = TimeDuration.ZERO;
    TimeDuration toRequest = TimeDuration.ZERO;

    for(int i = 0; i < n; i++) {
      final ByteString bytes = newByteString(messageSize.getSizeInt(), i);
      final RaftClientRequest request = RaftClientRequest.newBuilder()
          .setClientId(clientId)
          .setServerId(leaderId)
          .setGroupId(groupId)
          .setCallId(1)
          .setMessage(() -> bytes)
          .setType(RaftClientRequest.writeRequestType())
          .build();
      final Timestamp startTime = Timestamp.currentTime();
      final RaftClientRequestProto proto = ClientProtoUtils.toRaftClientRequestProto(request);
      final TimeDuration p = startTime.elapsedTime();
      final RaftClientRequest computed = ClientProtoUtils.toRaftClientRequest(proto);
      final TimeDuration r = startTime.elapsedTime().subtract(p);

      Assertions.assertEquals(request.getMessage().getContent(), computed.getMessage().getContent());
      toProto = toProto.add(p);
      toRequest = toRequest.add(r);

    }

    System.out.printf("%nmessageSize=%s, n=%d%n", messageSize, n);
    print("toProto  ", toProto, n);
    print("toRequest", toRequest, n);
  }

  void print(String name, TimeDuration t, int n) {
    final long ns = t.toLong(TimeUnit.NANOSECONDS);
    System.out.printf("%s: avg = %s (total = %s)%n", name, ns2String(ns/n), ns2String(ns));
  }

  static String ns2String(long ns) {
    return String.format("%.3fms", ns/1_000_000.0);
  }

  static ByteString newByteString(int size, int offset) throws IOException {
    try(final ByteString.Output out = ByteString.newOutput()) {
      for (int i = 0; i < size; i++) {
        out.write(i + offset);
      }
      return out.toByteString();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testGroupInfoReplyProtoRoundTrip(boolean withConf) {
    final ClientId clientId = ClientId.randomId();
    final RaftPeerId serverId = RaftPeerId.valueOf("s0");
    final RaftGroupId groupId = RaftGroupId.randomId();
    final RaftPeer peer = RaftPeer.newBuilder().setId(serverId).setAddress("127.0.0.1:1234").build();
    final RaftGroup group = RaftGroup.valueOf(groupId, peer);

    final RaftConfigurationProto conf = withConf ? newRaftConfigurationProto(peer) : null;

    final GroupInfoReply original = new GroupInfoReply(clientId, serverId, groupId, 1L,
        Collections.singletonList(newCommitInfoProto(peer, 100L)), group, newRoleInfoProto(peer), true,
        conf, newLogInfoProto());

    final GroupInfoReplyProto proto = ClientProtoUtils.toGroupInfoReplyProto(original);
    final GroupInfoReply computed = ClientProtoUtils.toGroupInfoReply(proto);

    assertGroupInfoReply(original, computed);
  }

  private static void assertGroupInfoReply(GroupInfoReply expected, GroupInfoReply actual) {
    Assertions.assertEquals(expected.getClientId(), actual.getClientId());
    Assertions.assertEquals(expected.getServerId(), actual.getServerId());
    Assertions.assertEquals(expected.getRaftGroupId(), actual.getRaftGroupId());
    Assertions.assertEquals(expected.getCallId(), actual.getCallId());
    Assertions.assertEquals(expected.getCommitInfos(), actual.getCommitInfos());
    Assertions.assertEquals(expected.getGroup(), actual.getGroup());
    Assertions.assertEquals(expected.getRoleInfoProto(), actual.getRoleInfoProto());
    Assertions.assertEquals(expected.isRaftStorageHealthy(), actual.isRaftStorageHealthy());
    Assertions.assertEquals(expected.getConf(), actual.getConf());
    Assertions.assertEquals(expected.getLogInfoProto(), actual.getLogInfoProto());
  }

  private static RaftConfigurationProto newRaftConfigurationProto(RaftPeer peer) {
    return RaftConfigurationProto.newBuilder()
        .addPeers(peer.getRaftPeerProto())
        .addOldPeers(newRaftPeerProto("s1", "127.0.0.1:1235"))
        .addListeners(newRaftPeerProto("s2", "127.0.0.1:1236"))
        .addOldListeners(newRaftPeerProto("s3", "127.0.0.1:1237"))
        .build();
  }

  private static RaftPeerProto newRaftPeerProto(String id, String address) {
    return RaftPeer.newBuilder().setId(id).setAddress(address).build().getRaftPeerProto();
  }

  private static CommitInfoProto newCommitInfoProto(RaftPeer peer, long commitIndex) {
    return CommitInfoProto.newBuilder()
        .setServer(peer.getRaftPeerProto())
        .setCommitIndex(commitIndex)
        .build();
  }

  private static RoleInfoProto newRoleInfoProto(RaftPeer peer) {
    return RoleInfoProto.newBuilder()
        .setSelf(peer.getRaftPeerProto())
        .setRole(RaftPeerRole.LEADER)
        .setRoleElapsedTimeMs(1000L)
        .setLeaderInfo(LeaderInfoProto.newBuilder().setTerm(1L).build())
        .build();
  }

  private static LogInfoProto newLogInfoProto() {
    return LogInfoProto.newBuilder()
        .setLastSnapshot(newTermIndexProto(1L, 10L))
        .setApplied(newTermIndexProto(1L, 20L))
        .setCommitted(newTermIndexProto(1L, 30L))
        .setLastEntry(newTermIndexProto(1L, 40L))
        .build();
  }

  private static TermIndexProto newTermIndexProto(long term, long index) {
    return TermIndexProto.newBuilder().setTerm(term).setIndex(index).build();
  }
}
