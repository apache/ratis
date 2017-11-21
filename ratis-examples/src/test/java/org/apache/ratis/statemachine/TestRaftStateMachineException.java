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
import org.apache.ratis.MiniRaftCluster;
import org.apache.ratis.RaftTestUtil.SimpleMessage;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientRpc;
import org.apache.ratis.examples.ParameterizedBaseTest;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.impl.RaftServerTestUtil;
import org.apache.ratis.server.impl.RetryCache;
import org.apache.ratis.server.storage.RaftLog;
import org.apache.ratis.util.LogUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.fail;

public class TestRaftStateMachineException extends ParameterizedBaseTest {
  static {
    LogUtils.setLogLevel(RaftServerImpl.LOG, Level.DEBUG);
    LogUtils.setLogLevel(RaftLog.LOG, Level.DEBUG);
    LogUtils.setLogLevel(RaftClient.LOG, Level.DEBUG);
  }

  protected static boolean failPreAppend = false;

  protected static class StateMachineWithException extends SimpleStateMachine4Testing {
    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
      CompletableFuture<Message> future = new CompletableFuture<>();
      future.completeExceptionally(new StateMachineException("Fake Exception"));
      return future;
    }

    @Override
    public TransactionContext preAppendTransaction(TransactionContext trx)
        throws IOException {
      if (failPreAppend) {
        throw new IOException("Fake Exception in preAppend");
      } else {
        return trx;
      }
    }
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() throws IOException {
    return getMiniRaftClusters(StateMachineWithException.class, 3);
  }

  @Parameterized.Parameter
  public MiniRaftCluster cluster;

  @Test
  public void testHandleStateMachineException() throws Exception {
    setAndStart(cluster);

    final RaftPeerId leaderId = cluster.getLeader().getId();

    try(final RaftClient client = cluster.createClient(leaderId)) {
      client.send(new SimpleMessage("m"));
      fail("Exception expected");
    } catch (StateMachineException e) {
      e.printStackTrace();
      Assert.assertTrue(e.getCause().getMessage().contains("Fake Exception"));
    }
  }

  @Test
  public void testRetryOnStateMachineException() throws Exception {
    setAndStart(cluster);

    final RaftPeerId leaderId = cluster.getLeaderAndSendFirstMessage(true).getId();
    long oldLastApplied = cluster.getLeader().getState().getLastAppliedIndex();

    final RaftClient client = cluster.createClient(leaderId);
    final RaftClientRpc rpc = client.getClientRpc();
    final long callId = 999;
    RaftClientRequest r = new RaftClientRequest(client.getId(), leaderId,
        cluster.getGroupId(), callId, new SimpleMessage("message"));
    RaftClientReply reply = rpc.sendRequest(r);
    Assert.assertFalse(reply.isSuccess());
    Assert.assertNotNull(reply.getStateMachineException());

    // retry with the same callId
    for (int i = 0; i < 5; i++) {
      reply = rpc.sendRequest(r);
      Assert.assertEquals(client.getId(), reply.getClientId());
      Assert.assertEquals(callId, reply.getCallId());
      Assert.assertFalse(reply.isSuccess());
      Assert.assertNotNull(reply.getStateMachineException());
    }

    long leaderApplied = cluster.getLeader().getState().getLastAppliedIndex();
    // make sure retry cache has the entry
    for (RaftServerImpl server : cluster.iterateServerImpls()) {
      LOG.info("check server " + server.getId());
      if (server.getState().getLastAppliedIndex() < leaderApplied) {
        Thread.sleep(1000);
      }
      Assert.assertNotNull(
          RaftServerTestUtil.getRetryEntry(server, client.getId(), callId));
      Assert.assertEquals(oldLastApplied + 1,
          server.getState().getLastAppliedIndex());
    }

    client.close();
  }

  @Test
  public void testRetryOnExceptionDuringReplication() throws Exception {
    setAndStart(cluster);
    final RaftPeerId leaderId = cluster.getLeaderAndSendFirstMessage(true).getId();

    // turn on the preAppend failure switch
    failPreAppend = true;
    final RaftClient client = cluster.createClient(leaderId);
    final RaftClientRpc rpc = client.getClientRpc();
    final long callId = 999;
    RaftClientRequest r = new RaftClientRequest(client.getId(), leaderId,
        cluster.getGroupId(), callId, new SimpleMessage("message"));
    RaftClientReply reply = rpc.sendRequest(r);
    Assert.assertTrue(reply.hasStateMachineException());

    RetryCache.CacheEntry oldEntry = RaftServerTestUtil.getRetryEntry(
        cluster.getLeader(), client.getId(), callId);
    Assert.assertNotNull(oldEntry);
    Assert.assertTrue(RaftServerTestUtil.isRetryCacheEntryFailed(oldEntry));

    // retry
    reply = rpc.sendRequest(r);
    Assert.assertTrue(reply.hasStateMachineException());

    RetryCache.CacheEntry currentEntry = RaftServerTestUtil.getRetryEntry(
        cluster.getLeader(), client.getId(), callId);
    Assert.assertNotNull(currentEntry);
    Assert.assertTrue(RaftServerTestUtil.isRetryCacheEntryFailed(currentEntry));
    Assert.assertNotEquals(oldEntry, currentEntry);

    failPreAppend = false;
    client.close();
  }
}
