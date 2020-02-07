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
package org.apache.ratis;

import org.apache.log4j.Level;
import org.apache.ratis.RaftTestUtil.SimpleMessage;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.MessageOutputStream;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.statemachine.SimpleStateMachine4Testing;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.Log4jUtils;
import org.junit.Assert;
import org.junit.Test;

public abstract class StreamApiTests<CLUSTER extends MiniRaftCluster> extends BaseTest
    implements MiniRaftCluster.Factory.Get<CLUSTER> {
  {
    Log4jUtils.setLogLevel(RaftServerImpl.LOG, Level.DEBUG);
    Log4jUtils.setLogLevel(RaftClient.LOG, Level.DEBUG);
  }

  public static final int NUM_SERVERS = 3;

  {
    getProperties().setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        SimpleStateMachine4Testing.class, StateMachine.class);
  }

  @Test
  public void testStream() throws Exception {
    runWithNewCluster(NUM_SERVERS, this::runTestStream);
  }

  void runTestStream(CLUSTER cluster) throws Exception {
    RaftTestUtil.waitForLeader(cluster);

    // stream multiple parts
    final int numParts = 5;
    final StringBuilder key = new StringBuilder();
    try(RaftClient client = cluster.createClient();
        MessageOutputStream out = client.getStreamApi().stream()) {
      for (int i = 0; i < numParts; i++) {
        key.append(i);
        out.sendAsync(new SimpleMessage(i + ""));
      }
    }

    // check if all the parts are streamed as a single message.
    try(RaftClient client = cluster.createClient()) {
      final RaftClientReply reply = client.sendReadOnly(new SimpleMessage(key.toString()));
      Assert.assertTrue(reply.isSuccess());
    }
  }
}
