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
package org.apache.ratis.server.simulation;

import org.apache.ratis.MiniRaftCluster;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;

public class MiniRaftClusterWithSimulatedRpc extends MiniRaftCluster {
  static final Logger LOG = LoggerFactory.getLogger(MiniRaftClusterWithSimulatedRpc.class);

  public static final Factory<MiniRaftClusterWithSimulatedRpc> FACTORY
      = new Factory<MiniRaftClusterWithSimulatedRpc>() {
    @Override
    public MiniRaftClusterWithSimulatedRpc newCluster(
        String[] ids, RaftProperties prop, boolean formatted) {
      RaftConfigKeys.Rpc.setType(prop::set, SimulatedRpc.INSTANCE);
      if (ThreadLocalRandom.current().nextBoolean()) {
        // turn off simulate latency half of the times.
        prop.setInt(SimulatedRequestReply.SIMULATE_LATENCY_KEY, 0);
      }
      return new MiniRaftClusterWithSimulatedRpc(ids, prop, formatted);
    }
  };

  private SimulatedRequestReply<RaftServerRequest, RaftServerReply> serverRequestReply;
  private SimulatedClientRequestReply client2serverRequestReply;

  private MiniRaftClusterWithSimulatedRpc(String[] ids,
      RaftProperties properties, boolean formatted) {
    super(ids, properties, formatted);
    initRpc();
  }

  @Override
  protected void initRpc() {
    final int simulateLatencyMs = properties.getInt(
        SimulatedRequestReply.SIMULATE_LATENCY_KEY,
        SimulatedRequestReply.SIMULATE_LATENCY_DEFAULT);
    LOG.info(SimulatedRequestReply.SIMULATE_LATENCY_KEY + " = "
        + simulateLatencyMs);
    serverRequestReply = new SimulatedRequestReply<>(simulateLatencyMs);
    client2serverRequestReply = new SimulatedClientRequestReply(simulateLatencyMs);
    getServers().stream().forEach(s -> initRpc(s));
    addPeersToRpc(toRaftPeers(getServers()));
    ((SimulatedRpc.Factory)clientFactory).initRpc(
        serverRequestReply, client2serverRequestReply);
  }

  private void initRpc(RaftServerImpl s) {
    if (serverRequestReply != null) {
      ((SimulatedRpc.Factory)s.getFactory()).initRpc(
          serverRequestReply, client2serverRequestReply);
    }
  }

  private void addPeersToRpc(Collection<RaftPeer> peers) {
    serverRequestReply.addPeers(peers);
    client2serverRequestReply.addPeers(peers);
  }

  @Override
  protected RaftServerImpl newRaftServer(RaftPeerId id, boolean format) {
    final RaftServerImpl s = super.newRaftServer(id, format);
    initRpc(s);
    return s;
  }

  @Override
  public void restartServer(String id, boolean format) throws IOException {
    super.restartServer(id, format);
    RaftServerImpl s = getServer(id);
    addPeersToRpc(Collections.singletonList(conf.getPeer(new RaftPeerId(id))));
    s.start();
  }

  @Override
  public Collection<RaftPeer> addNewPeers(
      Collection<RaftServerImpl> newServers, boolean startService) {
    final Collection<RaftPeer> newPeers = toRaftPeers(newServers);
    addPeersToRpc(newPeers);
    if (startService) {
      newServers.forEach(RaftServerImpl::start);
    }
    return newPeers;
  }

  @Override
  public void blockQueueAndSetDelay(String leaderId, int delayMs)
      throws InterruptedException {
    // block leader sendRequest if delayMs > 0
    final boolean block = delayMs > 0;
    LOG.debug("{} leader queue {} and set {}ms delay for the other queues",
        block? "Block": "Unblock", leaderId, delayMs);
    serverRequestReply.getQueue(leaderId).blockSendRequestTo.set(block);

    // set delay takeRequest for the other queues
    getServers().stream().filter(s -> !s.getId().toString().equals(leaderId))
        .map(s -> serverRequestReply.getQueue(s.getId().toString()))
        .forEach(q -> q.delayTakeRequestTo.set(delayMs));

    final long sleepMs = 3 * getMaxTimeout() / 2;
    Thread.sleep(sleepMs);
  }

  @Override
  public void setBlockRequestsFrom(String src, boolean block) {
    serverRequestReply.getQueue(src).blockTakeRequestFrom.set(block);
  }
}
