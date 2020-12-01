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

import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.examples.ParameterizedBaseTest;
import org.apache.ratis.examples.counter.server.CounterStateMachine;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collection;

public class TestCounter extends ParameterizedBaseTest {

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return getMiniRaftClusters(CounterStateMachine.class, 3);
  }

  @Parameterized.Parameter
  public MiniRaftCluster cluster;

  @Test
  public void testSeveralCounter() throws IOException, InterruptedException {
    setAndStart(cluster);
    try (final RaftClient client = cluster.createClient()) {
      for (int i = 0; i < 10; i++) {
        client.io().send(Message.valueOf("INCREMENT"));
      }
      RaftClientReply reply1 = client.io().sendReadOnly(Message.valueOf("GET"));
      Assert.assertEquals("10",
          reply1.getMessage().getContent().toString(Charset.defaultCharset()));
      for (int i = 0; i < 10; i++) {
        client.io().send(Message.valueOf("INCREMENT"));
      }
      RaftClientReply reply2 = client.io().sendReadOnly(Message.valueOf("GET"));
      Assert.assertEquals("20",
          reply2.getMessage().getContent().toString(Charset.defaultCharset()));
      for (int i = 0; i < 10; i++) {
        client.io().send(Message.valueOf("INCREMENT"));
      }
      RaftClientReply reply3 = client.io().sendReadOnly(Message.valueOf("GET"));
      Assert.assertEquals("30",
          reply3.getMessage().getContent().toString(Charset.defaultCharset()));
    }
  }
}
