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
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.examples.RaftExamplesTestUtil;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.StateMachineException;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.simulation.RequestHandler;
import org.apache.ratis.server.storage.RaftLog;
import org.apache.ratis.util.LogUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class TestRaftStateMachineException {
  static {
    LogUtils.setLogLevel(RaftServerImpl.LOG, Level.DEBUG);
    LogUtils.setLogLevel(RaftLog.LOG, Level.DEBUG);
    LogUtils.setLogLevel(RequestHandler.LOG, Level.DEBUG);
    LogUtils.setLogLevel(RaftClient.LOG, Level.DEBUG);
  }

  protected static class StateMachineWithException extends SimpleStateMachine4Testing {
    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
      CompletableFuture<Message> future = new CompletableFuture<>();
      future.completeExceptionally(new StateMachineException("Fake Exception"));
      return future;
    }
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() throws IOException {
    return RaftExamplesTestUtil.getMiniRaftClusters(
        StateMachineWithException.class);
  }

  @Parameterized.Parameter
  public MiniRaftCluster cluster;

  @Test
  public void testHandleStateMachineException() throws Exception {
    cluster.start();
    RaftTestUtil.waitForLeader(cluster);

    final RaftPeerId leaderId = cluster.getLeader().getId();

    try(final RaftClient client = cluster.createClient(leaderId)) {
      client.send(new RaftTestUtil.SimpleMessage("m"));
      fail("Exception expected");
    } catch (StateMachineException e) {
      e.printStackTrace();
      Assert.assertTrue(e.getCause().getMessage().contains("Fake Exception"));
    }

    cluster.shutdown();
  }
}
