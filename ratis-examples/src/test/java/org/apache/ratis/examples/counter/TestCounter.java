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

package org.apache.ratis.examples.counter;

import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.examples.ParameterizedBaseTest;
import org.apache.ratis.examples.counter.server.CounterStateMachine;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class TestCounter extends ParameterizedBaseTest {

  public static Collection<Object[]> data() {
    return getMiniRaftClusters(CounterStateMachine.class, 3);
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testSeveralCounter(MiniRaftCluster cluster) throws IOException, InterruptedException {
    setAndStart(cluster);
    try (final RaftClient client = cluster.createClient()) {
      for (int i = 0; i < 10; i++) {
        client.io().send(Message.valueOf("INCREMENT"));
      }
      RaftClientReply reply1 = client.io().sendReadOnly(Message.valueOf("GET"));
      Assertions.assertEquals(10, reply1.getMessage().getContent().asReadOnlyByteBuffer().getInt());
      for (int i = 0; i < 10; i++) {
        client.io().send(Message.valueOf("INCREMENT"));
      }
      RaftClientReply reply2 = client.io().sendReadOnly(Message.valueOf("GET"));
      Assertions.assertEquals(20, reply2.getMessage().getContent().asReadOnlyByteBuffer().getInt());
      for (int i = 0; i < 10; i++) {
        client.io().send(Message.valueOf("INCREMENT"));
      }
      RaftClientReply reply3 = client.io().sendReadOnly(Message.valueOf("GET"));
      Assertions.assertEquals(30, reply3.getMessage().getContent().asReadOnlyByteBuffer().getInt());
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testSeveralCounterFromFollower(MiniRaftCluster cluster) throws IOException, InterruptedException {
    setAndStart(cluster);

    List<RaftServer.Division> followers = cluster.getFollowers();
    Assertions.assertEquals(2, followers.size());
    final RaftPeerId f0 = followers.get(0).getId();
    final RaftServer.Division d1 = cluster.getDivision(f0);

    Message queryMessage = Message.valueOf("GET");

    try (final RaftClient client = cluster.createClient()) {
      for (int i = 0; i < 10; i++) {
        client.io().send(Message.valueOf("INCREMENT"));
      }
      RaftClientReply reply1 = client.io().sendReadOnly(queryMessage);
      Assertions.assertEquals(10,  getCounterInt(reply1.getMessage()));
      boolean isFollowerUptoDate = d1.okForLocalReadBounded(1000, 100);
      // Clients can choose to query from local StateMachine or leader
      if (isFollowerUptoDate) {
        d1.getStateMachine().query(queryMessage).get();
        Assertions.assertTrue(10 >= getCounterInt(d1.getStateMachine().query(queryMessage).get()));
      } else {
        reply1 = client.io().sendReadOnly(queryMessage);
        Assertions.assertEquals(10,  getCounterInt(reply1.getMessage()));
      }
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  private int getCounterInt(Message message) {
    return message.getContent().asReadOnlyByteBuffer().getInt();
  }
}
