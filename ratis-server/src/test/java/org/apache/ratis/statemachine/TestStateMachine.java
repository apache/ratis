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
package org.apache.ratis.statemachine;

import org.apache.log4j.Level;
import org.apache.ratis.BaseTest;
import org.apache.ratis.MiniRaftCluster;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.simulation.MiniRaftClusterWithSimulatedRpc;
import org.apache.ratis.proto.RaftProtos.SMLogEntryProto;
import org.apache.ratis.statemachine.impl.TransactionContextImpl;
import org.apache.ratis.util.LogUtils;
import org.junit.*;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
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
public class TestStateMachine extends BaseTest {
  static {
    LogUtils.setLogLevel(RaftServerImpl.LOG, Level.DEBUG);
    LogUtils.setLogLevel(RaftClient.LOG, Level.DEBUG);
  }

  public static final int NUM_SERVERS = 5;

  private final RaftProperties properties = new RaftProperties();
  {
    // TODO: fix and run with in-memory log. It fails with NPE
    // TODO: if change setUseMemory to true
    RaftServerConfigKeys.Log.setUseMemory(properties, false);
  }

  private MiniRaftClusterWithSimulatedRpc cluster;

  @Before
  public void setup() throws IOException {
    properties.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY, SMTransactionContext.class, StateMachine.class);

    cluster = MiniRaftClusterWithSimulatedRpc.FACTORY.newCluster(NUM_SERVERS, properties);
    cluster.start();
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  static class SMTransactionContext extends SimpleStateMachine4Testing {
    public static SMTransactionContext get(RaftServerImpl s) {
      return (SMTransactionContext)s.getStateMachine();
    }

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
      return new TransactionContextImpl(this, request, SMLogEntryProto.newBuilder()
          .setData(request.getMessage().getContent())
          .build(), transactions.incrementAndGet());
    }

    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
      try {
        assertNotNull(trx.getLogEntry());
        assertNotNull(trx.getSMLogEntry());
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
    int numTrx = 100;
    final RaftTestUtil.SimpleMessage[] messages = RaftTestUtil.SimpleMessage.create(numTrx);
    try(final RaftClient client = cluster.createClient()) {
      for (RaftTestUtil.SimpleMessage message : messages) {
        client.send(message);
      }
    }

    // TODO: there eshould be a better way to ensure all data is replicated and applied
    Thread.sleep(cluster.getMaxTimeout() + 100);

    for (RaftServerImpl raftServer : cluster.iterateServerImpls()) {
      final SMTransactionContext sm = SMTransactionContext.get(raftServer);
      sm.rethrowIfException();
      assertEquals(numTrx, sm.numApplied.get());
    }

    // check leader
    RaftServerImpl raftServer = cluster.getLeader();
    // assert every transaction has obtained context in leader
    final SMTransactionContext sm = SMTransactionContext.get(raftServer);
    List<Long> ll = sm.applied.stream().collect(Collectors.toList());
    Collections.sort(ll);
    assertEquals(ll.toString(), ll.size(), numTrx);
    for (int i=0; i < numTrx; i++) {
      assertEquals(ll.toString(), Long.valueOf(i+1), ll.get(i));
    }
  }
}
