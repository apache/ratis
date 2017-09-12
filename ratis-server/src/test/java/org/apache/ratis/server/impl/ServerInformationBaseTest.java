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
package org.apache.ratis.server.impl;

import org.apache.log4j.Level;
import org.apache.ratis.BaseTest;
import org.apache.ratis.MiniRaftCluster;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.ServerInformationReply;
import org.apache.ratis.util.LogUtils;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import static org.apache.ratis.util.Preconditions.assertTrue;

public abstract class ServerInformationBaseTest<CLUSTER extends MiniRaftCluster>
    extends BaseTest
    implements MiniRaftCluster.Factory.Get<CLUSTER> {
  static {
    LogUtils.setLogLevel(RaftServerImpl.LOG, Level.DEBUG);
    LogUtils.setLogLevel(RaftClient.LOG, Level.DEBUG);
  }

  static final RaftProperties prop = new RaftProperties();

  public MiniRaftCluster getCluster(int peerNum) throws IOException {
    return getFactory().newCluster(peerNum, prop);
  }

  @Test
  public void testServerInformation() throws Exception {
    runTest(5);
  }


  private void runTest(int num) throws Exception {
    LOG.info("Running server info test with " + num);

    final MiniRaftCluster cluster = getCluster(num);

    cluster.start();
    // all the peers in the cluster are in the same group, get it.
    RaftGroup group = cluster.getGroup();

    List<RaftPeer> peers = cluster.getPeers();
    // check that all the peers return exactly this group when group information
    // is requested.
    for (RaftPeer peer : peers) {
      try(final RaftClient client = cluster.createClient(peer.getId())) {
        RaftClientReply reply = client.serverInformation(peer.getId());
        assertTrue(reply instanceof ServerInformationReply);
        ServerInformationReply info = (ServerInformationReply)reply;
        assertTrue(sameGroup(group, info.getGroup()));
      }
    }
  }

  private boolean sameGroup(RaftGroup expected, RaftGroup given) {
    if (!expected.getGroupId().toString().equals(
        given.getGroupId().toString())) {
      return false;
    }
    List<RaftPeer> expectedPeers = expected.getPeers();
    List<RaftPeer> givenPeers = given.getPeers();
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
