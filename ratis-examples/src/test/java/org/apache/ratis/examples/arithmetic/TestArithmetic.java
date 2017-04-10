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
package org.apache.ratis.examples.arithmetic;


import org.apache.log4j.Level;
import org.apache.ratis.MiniRaftCluster;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.examples.RaftExamplesTestUtil;
import org.apache.ratis.examples.arithmetic.expression.*;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.util.LogUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Collection;

@RunWith(Parameterized.class)
public class TestArithmetic {
  static {
    LogUtils.setLogLevel(ArithmeticStateMachine.LOG, Level.ALL);
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() throws IOException {
    return RaftExamplesTestUtil.getMiniRaftClusters(ArithmeticStateMachine.class);
  }

  @Parameterized.Parameter
  public MiniRaftCluster cluster;

  @Test
  public void testPythagorean() throws Exception {
    cluster.start();
    RaftTestUtil.waitForLeader(cluster);
    final RaftPeerId leaderId = cluster.getLeader().getId();
    final RaftClient client = cluster.createClient(leaderId);

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

      RaftClientReply r;
      r = client.send(new AssignmentMessage(a, new DoubleValue(n)));
      assertRaftClientReply(r, (double)n);
      r = client.sendReadOnly(Expression.Utils.toMessage(a2));
      assertRaftClientReply(r, (double)n2);
      r = client.send(new AssignmentMessage(b, new DoubleValue(half_n2)));
      assertRaftClientReply(r, (double)half_n2);
      r = client.sendReadOnly(Expression.Utils.toMessage(b2));
      assertRaftClientReply(r, (double)half_n2*half_n2);
      r = client.send(pythagorean);
      assertRaftClientReply(r, (double)half_n2 + 1);

      r = client.send(nullA);
      assertRaftClientReply(r, null);
      r = client.send(nullB);
      assertRaftClientReply(r, null);
      r = client.send(nullC);
      assertRaftClientReply(r, null);
    }
    client.close();
    cluster.shutdown();
  }

  static void assertRaftClientReply(RaftClientReply reply, Double expected) {
    Assert.assertTrue(reply.isSuccess());
    final Expression e = Expression.Utils.bytes2Expression(
        reply.getMessage().getContent().toByteArray(), 0);
    Assert.assertEquals(expected, e.evaluate(null));
  }
}
