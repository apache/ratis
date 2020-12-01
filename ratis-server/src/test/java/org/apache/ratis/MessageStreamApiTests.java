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
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.client.api.MessageOutputStream;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.statemachine.SimpleStateMachine4Testing;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.Log4jUtils;
import org.apache.ratis.util.SizeInBytes;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public abstract class MessageStreamApiTests<CLUSTER extends MiniRaftCluster> extends BaseTest
    implements MiniRaftCluster.Factory.Get<CLUSTER> {
  {
    Log4jUtils.setLogLevel(RaftServer.Division.LOG, Level.DEBUG);
    Log4jUtils.setLogLevel(RaftClient.LOG, Level.DEBUG);
  }

  public static final int NUM_SERVERS = 3;

  @Test
  public void testStream() throws Exception {
    final RaftProperties p = getProperties();
    p.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        SimpleStateMachine4Testing.class, StateMachine.class);

    runWithNewCluster(NUM_SERVERS, this::runTestStream);
  }

  void runTestStream(CLUSTER cluster) throws Exception {
    RaftTestUtil.waitForLeader(cluster);

    // stream multiple parts
    final int numParts = 9;
    final int endOfRequest = 6;
    final StringBuilder key = new StringBuilder();
    try(RaftClient client = cluster.createClient();
        MessageOutputStream out = client.getMessageStreamApi().stream()) {
      for (int i = 1; i <= numParts; i++) {
        key.append(i);
        out.sendAsync(new SimpleMessage(i + ""), i == endOfRequest);
      }
    }

    // check if all the parts are streamed as a single message.
    final String k = key.toString();
    try(RaftClient client = cluster.createClient()) {
      final String k1 = k.substring(0, endOfRequest);
      final RaftClientReply r1= client.io().sendReadOnly(new SimpleMessage(k1));
      Assert.assertTrue(r1.isSuccess());

      final String k2 = k.substring(endOfRequest);
      final RaftClientReply r2 = client.io().sendReadOnly(new SimpleMessage(k2));
      Assert.assertTrue(r2.isSuccess());
    }
  }

  private static final SizeInBytes SUBMESSAGE_SIZE = SizeInBytes.ONE_KB;

  @Test
  public void testStreamAsync() throws Exception {
    final RaftProperties p = getProperties();
    RaftClientConfigKeys.MessageStream.setSubmessageSize(p, SUBMESSAGE_SIZE);
    p.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        SimpleStateMachine4Testing.class, StateMachine.class);

    runWithNewCluster(NUM_SERVERS, this::runTestStreamAsync);
    RaftClientConfigKeys.MessageStream.setSubmessageSize(p);
  }

  void runTestStreamAsync(CLUSTER cluster) throws Exception {
    RaftTestUtil.waitForLeader(cluster);

    ByteString bytes = ByteString.EMPTY;
    for(int i = 0; i < 10; ) {
      final String s = (char)('A' + i) + "1234567";
      LOG.info("s=" + s);
      final ByteString b = ByteString.copyFrom(s, StandardCharsets.UTF_8);
      Assert.assertEquals(8, b.size());
      for(int j = 0; j < 128; j++) {
        bytes = bytes.concat(b);
      }
      i++;
      Assert.assertEquals(i*SUBMESSAGE_SIZE.getSizeInt(), bytes.size());
    }

    try(RaftClient client = cluster.createClient()) {
      final RaftClientReply reply = client.getMessageStreamApi().streamAsync(Message.valueOf(bytes)).get();
      Assert.assertTrue(reply.isSuccess());
    }


    // check if all the parts are streamed as a single message.
    try(RaftClient client = cluster.createClient()) {
      final RaftClientReply reply = client.io().sendReadOnly(new SimpleMessage(bytes.toString(StandardCharsets.UTF_8)));
      Assert.assertTrue(reply.isSuccess());
    }
  }
}
