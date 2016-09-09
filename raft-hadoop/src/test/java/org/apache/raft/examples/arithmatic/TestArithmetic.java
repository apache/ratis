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
package org.apache.raft.examples.arithmatic;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.apache.raft.MiniRaftCluster;
import org.apache.raft.RaftTestUtil;
import org.apache.raft.client.RaftClient;
import org.apache.raft.conf.RaftProperties;
import org.apache.raft.examples.arithmatic.expression.*;
import org.apache.raft.hadoopRpc.RaftHadoopRpcTestUtil;
import org.apache.raft.server.RaftServerConfigKeys;
import org.apache.raft.server.StateMachine;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;

@RunWith(Parameterized.class)
public class TestArithmetic {
  static final Logger LOG = LoggerFactory.getLogger(TestArithmetic.class);

  static {
    GenericTestUtils.setLogLevel(ArithmeticStateMachine.LOG, Level.ALL);
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() throws IOException {
    final Configuration conf = new Configuration();
    conf.set(RaftServerConfigKeys.Ipc.ADDRESS_KEY, "0.0.0.0:0");

    final RaftProperties prop = new RaftProperties();
    prop.setClass(RaftServerConfigKeys.RAFT_SERVER_STATEMACHINE_CLASS_KEY,
        ArithmeticStateMachine.class, StateMachine.class);
    return RaftHadoopRpcTestUtil.getMiniRaftClusters(3, conf, prop);
  }

  @Parameterized.Parameter
  public MiniRaftCluster cluster;

  @Test
  public void testPythagorean() throws Exception {
    cluster.start();
    RaftTestUtil.waitForLeader(cluster);
    final String leaderId = cluster.getLeader().getId();
    final RaftClient client = cluster.createClient("pythagorean", leaderId);

    final Variable a = new Variable("a");
    final Variable b = new Variable("b");
    final Variable c = new Variable("c");
    final BinaryExpression a2 = new BinaryExpression(BinaryExpression.Op.MULT, a, a);
    final BinaryExpression b2 = new BinaryExpression(BinaryExpression.Op.MULT, b, b);
    final BinaryExpression c2 = new BinaryExpression(BinaryExpression.Op.ADD, a2, b2);
    final AssignmentMessage pythagorean = new AssignmentMessage(c,
        new UnaryExpression(UnaryExpression.Op.SQRT, c2));

    final AssignmentMessage nullA = new AssignmentMessage(a, NullValue.getInstance());
    final AssignmentMessage nullB = new AssignmentMessage(b, NullValue.getInstance());
    final AssignmentMessage nullC = new AssignmentMessage(c, NullValue.getInstance());

    for(int n = 3; n < 100; n += 2) {
      int n2 = n*n;
      int half_n2 = n2/2;
      client.send(new AssignmentMessage(a, new DoubleValue(n)));
      client.send(new AssignmentMessage(b, new DoubleValue(half_n2)));
      client.send(pythagorean);
      client.send(nullA);
      client.send(nullB);
      client.send(nullC);
    }
  }
}
