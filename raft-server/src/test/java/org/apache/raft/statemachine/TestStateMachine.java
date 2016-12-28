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
package org.apache.raft.statemachine;

import org.apache.log4j.Level;
import org.apache.raft.MiniRaftCluster;
import org.apache.raft.RaftTestUtil;
import org.apache.raft.client.RaftClient;
import org.apache.raft.conf.RaftProperties;
import org.apache.raft.protocol.Message;
import org.apache.raft.protocol.RaftClientRequest;
import org.apache.raft.server.RaftServer;
import org.apache.raft.server.RaftServerConfigKeys;
import org.apache.raft.server.simulation.MiniRaftClusterWithSimulatedRpc;
import org.apache.raft.shaded.proto.RaftProtos.SMLogEntryProto;
import org.apache.raft.util.RaftUtils;
import org.junit.*;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * Test StateMachine related functionality
 */
public class TestStateMachine {
  static {
    RaftUtils.setLogLevel(RaftServer.LOG, Level.DEBUG);
    RaftUtils.setLogLevel(RaftClient.LOG, Level.DEBUG);
  }

  public static final int NUM_SERVERS = 5;

  private final RaftProperties properties = new RaftProperties();
  {
    // TODO: fix and run with in-memory log. It fails with NPE
    properties.setBoolean(RaftServerConfigKeys.RAFT_SERVER_USE_MEMORY_LOG_KEY, false);
  }

  private MiniRaftClusterWithSimulatedRpc cluster;

  @Rule
  public Timeout globalTimeout = new Timeout(60 * 1000);

  @Before
  public void setup() throws IOException {
  }

  private void startCluster() {
    cluster = new MiniRaftClusterWithSimulatedRpc(NUM_SERVERS, properties);
    Assert.assertNull(getCluster().getLeader());
    getCluster().start();
  }

  @After
  public void tearDown() {
    final MiniRaftCluster cluster = getCluster();
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  public MiniRaftClusterWithSimulatedRpc getCluster() {
    return cluster;
  }

  public RaftProperties getProperties() {
    return properties;
  }

  public static class SMTransactionContext extends SimpleStateMachine4Testing {
    AtomicReference<Throwable> throwable = new AtomicReference<>(null);
    AtomicLong transactions = new AtomicLong(0);
    AtomicBoolean isLeader = new AtomicBoolean(false);
    AtomicLong numApplied = new AtomicLong(0);
    ConcurrentLinkedQueue<Long> applied = new ConcurrentLinkedQueue<>();

    @Override
    public TransactionContext startTransaction(RaftClientRequest request) throws IOException {
      // only leader will get this call
      isLeader.set(true);
      // send the next transaction id as the "context" from SM
      return new TransactionContext(this, request, SMLogEntryProto.newBuilder()
          .setData(request.getMessage().getContent())
          .build(), transactions.incrementAndGet());
    }

    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
      try {
        assertTrue(trx.getLogEntry().isPresent());
        assertTrue(trx.getSMLogEntry().isPresent());
        Optional<Object> context = trx.getStateMachineContext();
        if (isLeader.get()) {
          assertTrue(trx.getClientRequest().isPresent());
          assertTrue(context.isPresent());
          assertTrue(context.get() instanceof Long);
          Long val = (Long)context.get();
          assertTrue(val <= transactions.get());
          applied.add(val);
        } else {
          assertFalse(trx.getClientRequest().isPresent());
          assertFalse(context.isPresent());
        }
        numApplied.incrementAndGet();
      } catch (Throwable t) {
        throwable.set(t);
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
    // tests that the TrxContext set by the StateMachine in Leader is passed back to the SM
    properties.setClass(
        MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        SMTransactionContext.class, StateMachine.class);
    startCluster();

    int numTrx = 100;
    final RaftTestUtil.SimpleMessage[] messages = RaftTestUtil.SimpleMessage.create(numTrx);
    try(final RaftClient client = cluster.createClient("client", null)) {
      for (RaftTestUtil.SimpleMessage message : messages) {
        client.send(message);
      }
    }

    // TODO: there eshould be a better way to ensure all data is replicated and applied
    Thread.sleep(cluster.getMaxTimeout() + 100);

    for (RaftServer raftServer : cluster.getServers()) {
      SMTransactionContext sm = ((SMTransactionContext)raftServer.getStateMachine());
      sm.rethrowIfException();
      assertEquals(numTrx, sm.numApplied.get());
    }

    // check leader
    RaftServer raftServer = cluster.getLeader();
    // assert every transaction has obtained context in leader
    SMTransactionContext sm = ((SMTransactionContext)raftServer.getStateMachine());
    List<Long> ll = sm.applied.stream().collect(Collectors.toList());
    Collections.sort(ll);
    assertEquals(ll.toString(), ll.size(), numTrx);
    for (int i=0; i < numTrx; i++) {
      assertEquals(ll.toString(), Long.valueOf(i+1), ll.get(i));
    }
  }
}
