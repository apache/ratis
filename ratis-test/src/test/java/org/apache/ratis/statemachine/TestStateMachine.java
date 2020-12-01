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
package org.apache.ratis.statemachine;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Level;
import org.apache.ratis.BaseTest;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.simulation.MiniRaftClusterWithSimulatedRpc;
import org.apache.ratis.util.Log4jUtils;
import org.junit.*;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

/**
 * Test StateMachine related functionality
 */
public class TestStateMachine extends BaseTest implements MiniRaftClusterWithSimulatedRpc.FactoryGet {
  static {
    Log4jUtils.setLogLevel(RaftServer.Division.LOG, Level.DEBUG);
    Log4jUtils.setLogLevel(RaftClient.LOG, Level.DEBUG);
  }

  public static final int NUM_SERVERS = 3;

  static class SMTransactionContext extends SimpleStateMachine4Testing {
    public static SMTransactionContext get(RaftServer.Division s) {
      return (SMTransactionContext)s.getStateMachine();
    }

    AtomicReference<Throwable> throwable = new AtomicReference<>(null);
    AtomicLong transactions = new AtomicLong(0);
    AtomicBoolean isLeader = new AtomicBoolean(false);
    AtomicLong numApplied = new AtomicLong(0);
    ConcurrentLinkedQueue<Long> applied = new ConcurrentLinkedQueue<>();

    @Override
    public TransactionContext startTransaction(RaftClientRequest request) {
      // only leader will get this call
      isLeader.set(true);
      // send the next transaction id as the "context" from SM
      return TransactionContext.newBuilder()
          .setStateMachine(this)
          .setClientRequest(request)
          .setStateMachineContext(transactions.incrementAndGet())
          .build();
    }

    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
      try {
        assertNotNull(trx.getLogEntry());
        assertNotNull(trx.getStateMachineLogEntry());
        Object context = trx.getStateMachineContext();
        if (isLeader.get()) {
          assertNotNull(trx.getClientRequest());
          assertNotNull(context);
          assertTrue(context instanceof Long);
          Long val = (Long)context;
          assertTrue(val <= transactions.get());
          applied.add(val);
        } else {
          assertNull(trx.getClientRequest());
          assertNull(context);
        }
        numApplied.incrementAndGet();
      } catch (Exception e) {
        throwable.set(e);
      }
      return CompletableFuture.completedFuture(null);
    }

    void rethrowIfException() throws Throwable {
      Throwable t = throwable.get();
      if (t != null) {
        throw t;
      }
    }
  }

  @Test
  public void testTransactionContextIsPassedBack() throws Throwable {
    runTestTransactionContextIsPassedBack(false);
  }

  @Test
  public void testTransactionContextIsPassedBackUseMemory() throws Throwable {
    runTestTransactionContextIsPassedBack(true);
  }

  void runTestTransactionContextIsPassedBack(boolean useMemory) throws Throwable {
    final RaftProperties properties = new RaftProperties();
    properties.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY, SMTransactionContext.class, StateMachine.class);
    RaftServerConfigKeys.Log.setUseMemory(properties, useMemory);

    try(MiniRaftClusterWithSimulatedRpc cluster = getFactory().newCluster(NUM_SERVERS, properties)) {
      cluster.start();
      runTestTransactionContextIsPassedBack(cluster);
    }
  }

  static void runTestTransactionContextIsPassedBack(MiniRaftCluster cluster) throws Throwable {
    // tests that the TrxContext set by the StateMachine in Leader is passed back to the SM
    int numTrx = 100;
    final RaftTestUtil.SimpleMessage[] messages = RaftTestUtil.SimpleMessage.create(numTrx);
    try(final RaftClient client = cluster.createClient()) {
      for (RaftTestUtil.SimpleMessage message : messages) {
        client.io().send(message);
      }
    }

    // TODO: there eshould be a better way to ensure all data is replicated and applied
    Thread.sleep(cluster.getTimeoutMax().toLong(TimeUnit.MILLISECONDS) + 100);

    for (RaftServer.Division raftServer : cluster.iterateDivisions()) {
      final SMTransactionContext sm = SMTransactionContext.get(raftServer);
      sm.rethrowIfException();
      assertEquals(numTrx, sm.numApplied.get());
    }

    // check leader
    RaftServer.Division raftServer = cluster.getLeader();
    // assert every transaction has obtained context in leader
    final SMTransactionContext sm = SMTransactionContext.get(raftServer);
    final List<Long> ll = new ArrayList<>(sm.applied);
    Collections.sort(ll);
    assertEquals(ll.toString(), ll.size(), numTrx);
    for (int i=0; i < numTrx; i++) {
      assertEquals(ll.toString(), Long.valueOf(i+1), ll.get(i));
    }
  }

  @Test
  public void testStateMachineRegistry() throws Throwable {
    final Map<RaftGroupId, StateMachine> registry = new ConcurrentHashMap<>();
    registry.put(RaftGroupId.randomId(), new SimpleStateMachine4Testing());
    registry.put(RaftGroupId.randomId(), new SMTransactionContext());

    try(MiniRaftClusterWithSimulatedRpc cluster = newCluster(0)) {
      cluster.setStateMachineRegistry(registry::get);

      final RaftPeerId id = RaftPeerId.valueOf("s0");
      cluster.putNewServer(id, null, true);
      cluster.start();

      for(RaftGroupId gid : registry.keySet()) {
        final RaftGroup newGroup = RaftGroup.valueOf(gid, cluster.getPeers());
        LOG.info("add new group: " + newGroup);
        try (final RaftClient client = cluster.createClient(newGroup)) {
          for (RaftPeer p : newGroup.getPeers()) {
            client.getGroupManagementApi(p.getId()).add(newGroup);
          }
        }
      }

      final RaftServer server = cluster.getServer(id);
      for(Map.Entry<RaftGroupId, StateMachine> e: registry.entrySet()) {
        Assert.assertSame(e.getValue(), server.getDivision(e.getKey()).getStateMachine());
      }
    }
  }
}
