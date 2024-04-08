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
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.GroupManagementApi;
import org.apache.ratis.protocol.*;
import org.apache.ratis.proto.RaftProtos.CommitInfoProto;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.util.Slf4jUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.event.Level;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public abstract class GroupInfoBaseTest<CLUSTER extends MiniRaftCluster>
    extends BaseTest
    implements MiniRaftCluster.Factory.Get<CLUSTER> {
  {
    Slf4jUtils.setLogLevel(RaftServer.Division.LOG, Level.DEBUG);
    Slf4jUtils.setLogLevel(RaftClient.LOG, Level.DEBUG);
  }

  @Test
  public void testGroupInfo() throws Exception {
    runWithNewCluster(3, this::runTest);
  }

  private void runTest(CLUSTER cluster) throws Exception {
    // all the peers in the cluster are in the same group, get it.
    RaftGroup group = cluster.getGroup();

    List<RaftPeer> peers = cluster.getPeers();

    // Multi-raft with the second group
    RaftGroup group2 = RaftGroup.valueOf(RaftGroupId.randomId(), peers);
    for(RaftPeer peer : peers) {
      try(final RaftClient client = cluster.createClient(peer.getId())) {
        client.getGroupManagementApi(peer.getId()).add(group2);
      }
    }
    // check that all the peers return the list where both groups are included. And able to return GroupInfo
    // for each of them.
    for (RaftPeer peer : peers) {
      try (final RaftClient client = cluster.createClient(peer.getId())) {
        final GroupManagementApi api = client.getGroupManagementApi(peer.getId());
        final GroupListReply info = api.list();
        List<RaftGroupId> groupList = info.getGroupIds().stream()
            .filter(id -> group.getGroupId().equals(id)).collect(Collectors.toList());
        assert (groupList.size() == 1);
        final GroupInfoReply gi = api.info(groupList.get(0));
        assert (sameGroup(group, gi.getGroup()));
        groupList = info.getGroupIds().stream()
            .filter(id -> group2.getGroupId().equals(id)).collect(Collectors.toList());
        assert (groupList.size() == 1);
        final GroupInfoReply gi2 = api.info(groupList.get(0));
        assert (sameGroup(group2, gi2.getGroup()));
      }
    }

    final int numMessages = 5;
    final long maxCommit;
    {
      // send some messages and get max commit from the last reply
      final RaftClientReply reply = sendMessages(numMessages, cluster);
      maxCommit = reply.getCommitInfos().stream().mapToLong(CommitInfoProto::getCommitIndex).max().getAsLong();
    }
    // kill a follower
    final RaftPeerId killedFollower = cluster.getFollowers().iterator().next().getId();
    cluster.killServer(killedFollower);
    {
      // send more messages and check last reply
      final RaftClientReply reply = sendMessages(numMessages, cluster);
      for(CommitInfoProto i : reply.getCommitInfos()) {
        if (!RaftPeerId.valueOf(i.getServer().getId()).equals(killedFollower)) {
          Assert.assertTrue(i.getCommitIndex() > maxCommit);
        }
      }
    }

    // check getGroupList
    for(RaftPeer peer : peers) {
      if (peer.getId().equals(killedFollower)) {
        continue;
      }
      try(final RaftClient client = cluster.createClient(peer.getId())) {
        final GroupListReply info = client.getGroupManagementApi(peer.getId()).list();
        Assert.assertEquals(1, info.getGroupIds().stream().filter(id -> group.getGroupId().equals(id)).count());
        for(CommitInfoProto i : info.getCommitInfos()) {
          if (RaftPeerId.valueOf(i.getServer().getId()).equals(killedFollower)) {
            Assert.assertTrue(i.getCommitIndex() <= maxCommit);
          } else {
            Assert.assertTrue(i.getCommitIndex() > maxCommit);
          }
        }
      }
    }
  }

  RaftClientReply sendMessages(int n, MiniRaftCluster cluster) throws Exception {
    LOG.info("sendMessages: " + n);
    RaftClientReply reply = null;
    try(final RaftClient client = cluster.createClient()) {
      for(int i = 0; i < n; i++) {
        reply = client.io().send(Message.valueOf("m" + i));
      }
    }
    return reply;
  }

  private boolean sameGroup(RaftGroup expected, RaftGroup given) {
    if (!expected.getGroupId().toString().equals(
        given.getGroupId().toString())) {
      return false;
    }
    Collection<RaftPeer> expectedPeers = expected.getPeers();
    Collection<RaftPeer> givenPeers = given.getPeers();
    if (expectedPeers.size() != givenPeers.size()) {
      return false;
    }
    for (RaftPeer peerGiven : givenPeers) {
      boolean found = false;
      for (RaftPeer peerExpect : expectedPeers) {
        if (peerGiven.getId().toString().equals(
            peerExpect.getId().toString())) {
          found = true;
          break;
        }
      }
      if (!found) {
        return false;
      }
    }
    return true;
  }
}
