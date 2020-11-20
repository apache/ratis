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

import org.apache.log4j.Level;
import org.apache.ratis.MiniRaftCluster;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.ClientInvocationId;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.DataStreamMap;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Log4jUtils;
import org.apache.ratis.util.TimeDuration;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public class RaftServerTestUtil {
  static final Logger LOG = LoggerFactory.getLogger(RaftServerTestUtil.class);

  public static void setWatchRequestsLogLevel(Level level) {
    Log4jUtils.setLogLevel(WatchRequests.LOG, level);
  }
  public static void setPendingRequestsLogLevel(Level level) {
    Log4jUtils.setLogLevel(PendingRequests.LOG, level);
  }

  public static void waitAndCheckNewConf(MiniRaftCluster cluster,
      RaftPeer[] peers, int numOfRemovedPeers, Collection<RaftPeerId> deadPeers)
      throws Exception {
    final TimeDuration sleepTime = cluster.getTimeoutMax().apply(n -> n * (numOfRemovedPeers + 2));
    JavaUtils.attempt(() -> waitAndCheckNewConf(cluster, Arrays.asList(peers), deadPeers),
        10, sleepTime, "waitAndCheckNewConf", LOG);
  }
  private static void waitAndCheckNewConf(MiniRaftCluster cluster,
      Collection<RaftPeer> peers, Collection<RaftPeerId> deadPeers) {
    LOG.info("waitAndCheckNewConf: peers={}, deadPeers={}, {}", peers, deadPeers, cluster.printServers());
    Assert.assertNotNull(cluster.getLeader());

    int numIncluded = 0;
    int deadIncluded = 0;
    final RaftConfiguration current = RaftConfiguration.newBuilder()
        .setConf(peers).setLogEntryIndex(0).build();
    for (RaftServerImpl server : cluster.iterateServerImpls()) {
      LOG.info("checking {}", server);
      if (deadPeers != null && deadPeers.contains(server.getId())) {
        if (current.containsInConf(server.getId())) {
          deadIncluded++;
        }
        continue;
      }
      if (current.containsInConf(server.getId())) {
        numIncluded++;
        Assert.assertTrue(server.getRaftConf().isStable());
        Assert.assertTrue(server.getRaftConf().hasNoChange(peers));
      } else if (server.isAlive()) {
        // The server is successfully removed from the conf
        // It may not be shutdown since it may not be able to talk to the new leader (who is not in its conf).
        Assert.assertTrue(server.getRaftConf().isStable());
        Assert.assertFalse(server.getRaftConf().containsInConf(server.getId()));
      }
    }
    Assert.assertEquals(peers.size(), numIncluded + deadIncluded);
  }

  public static long getRetryCacheSize(RaftServerImpl server) {
    return server.getRetryCache().size();
  }

  public static RetryCache.CacheEntry getRetryEntry(RaftServerImpl server, ClientId clientId, long callId) {
    return server.getRetryCache().get(ClientInvocationId.valueOf(clientId, callId));
  }

  public static boolean isRetryCacheEntryFailed(RetryCache.CacheEntry entry) {
    return entry.isFailed();
  }

  public static RaftPeerRole getRole(RaftServerImpl server) {
    return server.getRole().getRaftPeerRole();
  }

  private static Optional<LeaderState> getLeaderState(RaftServerImpl server) {
    return server.getRole().getLeaderState();
  }

  public static Stream<LogAppender> getLogAppenders(RaftServerImpl server) {
    return getLeaderState(server).map(LeaderState::getLogAppenders).orElse(null);
  }

  public static void restartLogAppenders(RaftServerImpl server) {
    final LeaderState leaderState = getLeaderState(server).orElseThrow(
        () -> new IllegalStateException(server + " is not the leader"));
    leaderState.getLogAppenders().forEach(leaderState::restartSender);
  }

  public static Logger getStateMachineUpdaterLog() {
    return StateMachineUpdater.LOG;
  }

  public static List<RaftServerImpl> getRaftServerImpls(RaftServerProxy proxy) {
    return JavaUtils.callAsUnchecked(proxy::getImpls);
  }

  public static RaftServerImpl getRaftServerImpl(RaftServerProxy proxy, RaftGroupId groupId) {
    return JavaUtils.callAsUnchecked(() -> proxy.getImpl(groupId));
  }

  public static DataStreamMap newDataStreamMap(Object name) {
    return new DataStreamMapImpl(name);
  }
}
