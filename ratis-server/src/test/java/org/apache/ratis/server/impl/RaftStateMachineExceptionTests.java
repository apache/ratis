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
import org.apache.ratis.BaseTest;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.RaftTestUtil.SimpleMessage;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientRpc;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.*;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RetryCache;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.statemachine.SimpleStateMachine4Testing;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Log4jUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.fail;

public abstract class RaftStateMachineExceptionTests<CLUSTER extends MiniRaftCluster>
    extends BaseTest implements MiniRaftCluster.Factory.Get<CLUSTER> {
  {
    Log4jUtils.setLogLevel(RaftServer.Division.LOG, Level.DEBUG);
    Log4jUtils.setLogLevel(RaftLog.LOG, Level.DEBUG);
    Log4jUtils.setLogLevel(RaftClient.LOG, Level.DEBUG);
  }

  private static volatile boolean failPreAppend = false;

  protected static class StateMachineWithException extends
      SimpleStateMachine4Testing {

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

  {
    final RaftProperties prop = getProperties();
    prop.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY, StateMachineWithException.class, StateMachine.class);
  }

  @Test
  public void testHandleStateMachineException() throws Exception {
    runWithNewCluster(3, this::runTestHandleStateMachineException);
  }

  private void runTestHandleStateMachineException(CLUSTER cluster) throws Exception {
    RaftPeerId leaderId = RaftTestUtil.waitForLeader(cluster).getId();

    try(final RaftClient client = cluster.createClient(leaderId)) {
      client.io().send(new RaftTestUtil.SimpleMessage("m"));
      fail("Exception expected");
    } catch (StateMachineException e) {
      e.printStackTrace();
      Assert.assertTrue(e.getCause().getMessage().contains("Fake Exception"));
    }
    cluster.shutdown();
  }

  @Test
  public void testRetryOnStateMachineException() throws Exception {
    runWithNewCluster(3, this::runTestRetryOnStateMachineException);
  }

  private void runTestRetryOnStateMachineException(CLUSTER cluster) throws Exception {
    RaftPeerId leaderId = RaftTestUtil.waitForLeader(cluster).getId();

    cluster.getLeaderAndSendFirstMessage(true);
    final long oldLastApplied = cluster.getLeader().getInfo().getLastAppliedIndex();

    try (final RaftClient client = cluster.createClient(leaderId)) {
      final RaftClientRpc rpc = client.getClientRpc();
      final long callId = 999;
      final SimpleMessage message = new SimpleMessage("message");
      final RaftClientRequest r = cluster.newRaftClientRequest(client.getId(), leaderId, callId, message);
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

      for (RaftServer.Division server : cluster.iterateDivisions()) {
        LOG.info("check server " + server.getId());

        JavaUtils.attemptRepeatedly(() -> {
          Assert.assertNotNull(RetryCacheTestUtil.get(server, client.getId(), callId));
          return null;
        }, 5, BaseTest.ONE_SECOND, "GetRetryEntry", LOG);

        final RaftLog log = server.getRaftLog();
        RaftTestUtil.logEntriesContains(log, oldLastApplied + 1, log.getNextIndex(), message);
      }

      cluster.shutdown();
    }
  }

  @Test
  public void testRetryOnExceptionDuringReplication() throws Exception {
    runWithNewCluster(3, this::runTestRetryOnExceptionDuringReplication);
  }

  private void runTestRetryOnExceptionDuringReplication(CLUSTER cluster) throws Exception {
    final RaftServer.Division oldLeader = RaftTestUtil.waitForLeader(cluster);
    cluster.getLeaderAndSendFirstMessage(true);
    // turn on the preAppend failure switch
    failPreAppend = true;
    try (final RaftClient client = cluster.createClient(oldLeader.getId())) {
      final RaftClientRpc rpc = client.getClientRpc();
      final long callId = 999;
      final SimpleMessage message = new SimpleMessage("message");
      RaftClientRequest r = cluster.newRaftClientRequest(client.getId(), oldLeader.getId(), callId, message);
      RaftClientReply reply = rpc.sendRequest(r);
      Objects.requireNonNull(reply.getStateMachineException());

      final RetryCache.Entry oldEntry = RetryCacheTestUtil.get(oldLeader, client.getId(), callId);
      Assert.assertNotNull(oldEntry);
      Assert.assertTrue(RetryCacheTestUtil.isFailed(oldEntry));

      // At this point of time the old leader would have stepped down. wait for leader election to complete
      final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster);
      // retry
      r = cluster.newRaftClientRequest(client.getId(), leader.getId(), callId, message);
      reply = rpc.sendRequest(r);
      Objects.requireNonNull(reply.getStateMachineException());

      final RetryCache.Entry currentEntry = RetryCacheTestUtil.get(leader, client.getId(), callId);
      Assert.assertNotNull(currentEntry);
      Assert.assertTrue(RetryCacheTestUtil.isFailed(currentEntry));
      Assert.assertNotEquals(oldEntry, currentEntry);
      failPreAppend = false;
    }
  }
}
