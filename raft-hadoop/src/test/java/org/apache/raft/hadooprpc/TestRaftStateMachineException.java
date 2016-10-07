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
package org.apache.raft.hadooprpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.apache.raft.MiniRaftCluster;
import org.apache.raft.RaftTestUtil;
import org.apache.raft.client.RaftClient;
import org.apache.raft.conf.RaftProperties;
import org.apache.raft.proto.RaftProtos;
import org.apache.raft.protocol.Message;
import org.apache.raft.server.RaftServer;
import org.apache.raft.server.RaftServerConfigKeys;
import org.apache.raft.server.SimpleStateMachine;
import org.apache.raft.server.StateMachine;
import org.apache.raft.protocol.StateMachineException;
import org.apache.raft.server.simulation.RequestHandler;
import org.apache.raft.server.storage.RaftLog;
import org.apache.raft.statemachine.TrxContext;
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
    GenericTestUtils.setLogLevel(RaftServer.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(RaftLog.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(RequestHandler.LOG, Level.DEBUG);
    GenericTestUtils.setLogLevel(RaftClient.LOG, Level.DEBUG);
  }

  private static class StateMachineWithException extends SimpleStateMachine {
    @Override
    public CompletableFuture<Message> applyLogEntry(TrxContext trx) {
      RaftProtos.LogEntryProto entry = trx.getLogEntry().get();
      CompletableFuture<Message> future = new CompletableFuture<>();
      future.completeExceptionally(new StateMachineException("Fake Exception"));
      return future;
    }
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() throws IOException {
    final Configuration conf = new Configuration();
    conf.set(RaftServerConfigKeys.Ipc.ADDRESS_KEY, "0.0.0.0:0");

    RaftProperties prop = new RaftProperties();
    prop.setClass(RaftServerConfigKeys.RAFT_SERVER_STATEMACHINE_CLASS_KEY,
        StateMachineWithException.class, StateMachine.class);

    return RaftHadoopRpcTestUtil.getMiniRaftClusters(3, conf, prop);
  }

  @Parameterized.Parameter
  public MiniRaftCluster cluster;

  @Test
  public void testHandleStateMachineException() throws Exception {
    cluster.start();
    RaftTestUtil.waitForLeader(cluster);

    final String leaderId = cluster.getLeader().getId();
    final RaftClient client = cluster.createClient("client", leaderId);

    try {
      client.send(new RaftTestUtil.SimpleMessage("m"));
      fail("Exception expected");
    } catch (StateMachineException e) {
      Assert.assertTrue(e.getMessage().contains("Fake Exception"));
    }
  }
}
