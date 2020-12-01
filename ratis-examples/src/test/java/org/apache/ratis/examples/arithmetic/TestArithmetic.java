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
package org.apache.ratis.examples.arithmetic;

import org.apache.log4j.Level;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.examples.ParameterizedBaseTest;
import org.apache.ratis.examples.arithmetic.expression.DoubleValue;
import org.apache.ratis.examples.arithmetic.expression.Expression;
import org.apache.ratis.examples.arithmetic.expression.NullValue;
import org.apache.ratis.examples.arithmetic.expression.Variable;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.util.Log4jUtils;
import org.apache.ratis.util.Preconditions;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Collection;

import static org.apache.ratis.examples.arithmetic.expression.BinaryExpression.Op.*;
import static org.apache.ratis.examples.arithmetic.expression.UnaryExpression.Op.SQRT;
import static org.apache.ratis.examples.arithmetic.expression.UnaryExpression.Op.SQUARE;

public class TestArithmetic extends ParameterizedBaseTest {
  {
    Log4jUtils.setLogLevel(ArithmeticStateMachine.LOG, Level.DEBUG);
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return getMiniRaftClusters(ArithmeticStateMachine.class, 3);
  }

  @Parameterized.Parameter
  public MiniRaftCluster cluster;

  @Test
  public void testPythagorean() throws Exception {
    setAndStart(cluster);
    try (final RaftClient client = cluster.createClient()) {
      runTestPythagorean(client, 3, 10);
    }
  }

  public static void runTestPythagorean(
      RaftClient client, int start, int count) throws IOException {
    Preconditions.assertTrue(count > 0, () -> "count = " + count + " <= 0");
    Preconditions.assertTrue(start >= 2, () -> "start = " + start + " < 2");

    final Variable a = new Variable("a");
    final Variable b = new Variable("b");
    final Variable c = new Variable("c");
    final Expression pythagorean = SQRT.apply(ADD.apply(SQUARE.apply(a), SQUARE.apply(b)));

    final int end = start + 2*count;
    for(int n = (start & 1) == 0? start + 1: start; n < end; n += 2) {
      int n2 = n*n;
      int half_n2 = n2/2;

      assign(client, a, n);
      assign(client, b, half_n2);
      assign(client, c, pythagorean, (double)half_n2 + 1);

      assignNull(client, a);
      assignNull(client, b);
      assignNull(client, c);
    }
  }

  @Test
  public void testGaussLegendre() throws Exception {
    setAndStart(cluster);
    try (final RaftClient client = cluster.createClient()) {
      runGaussLegendre(client);
    }
  }

  void runGaussLegendre(RaftClient client) throws IOException {
    defineVariable(client, "a0", 1);
    defineVariable(client, "b0", DIV.apply(1, SQRT.apply(2)));
    defineVariable(client, "t0", DIV.apply(1, 4));
    defineVariable(client, "p0", 1);

    double previous = 0;
    boolean converged = false;
    for(int i = 1; i < 8; i++) {
      final int i_1 = i - 1;
      final Variable a0 = new Variable("a" + i_1);
      final Variable b0 = new Variable("b" + i_1);
      final Variable t0 = new Variable("t" + i_1);
      final Variable p0 = new Variable("p" + i_1);
      final Variable a1 = defineVariable(client, "a"+i, DIV.apply(ADD.apply(a0, b0), 2));
      final Variable b1 = defineVariable(client, "b"+i, SQRT.apply(MULT.apply(a0, b0)));
      final Variable t1 = defineVariable(client, "t"+i, SUBTRACT.apply(t0, MULT.apply(p0, SQUARE.apply(SUBTRACT.apply(a0, a1)))));
      final Variable p1 = defineVariable(client, "p"+i, MULT.apply(2, p0));

      final Variable pi_i = new Variable("pi_"+i);
      final Expression e = assign(client, pi_i, DIV.apply(SQUARE.apply(a1), t0));
      final double pi = e.evaluate(null);

      if (converged) {
        Assert.assertTrue(pi == previous);
      } else if (pi == previous) {
        converged = true;
      }
      LOG.info("{} = {}, converged? {}", pi_i, pi, converged);
      previous = pi;
    }
    Assert.assertTrue(converged);
  }

  static Variable defineVariable(RaftClient client, String name, double value) throws IOException {
    final Variable x = new Variable(name);
    assign(client, x, value);
    return x;
  }

  static Variable defineVariable(RaftClient client, String name, Expression e) throws IOException {
    final Variable x = new Variable(name);
    assign(client, x, e, null);
    return x;
  }

  static Expression assign(RaftClient client, Variable x, double value) throws IOException {
    return assign(client, x, new DoubleValue(value), value);
  }

  static void assignNull(RaftClient client, Variable x) throws IOException {
    final Expression e = assign(client, x, NullValue.getInstance());
    Assert.assertEquals(NullValue.getInstance(), e);
  }

  static Expression assign(RaftClient client, Variable x, Expression e) throws IOException {
    return assign(client, x, e, null);
  }

  static Expression assign(RaftClient client, Variable x, Expression e, Double expected) throws IOException {
    final RaftClientReply r = client.io().send(x.assign(e));
    return assertRaftClientReply(r, expected);
  }

  static Expression assertRaftClientReply(RaftClientReply reply, Double expected) {
    Assert.assertTrue(reply.isSuccess());
    final Expression e = Expression.Utils.bytes2Expression(
        reply.getMessage().getContent().toByteArray(), 0);
    if (expected != null) {
      Assert.assertEquals(expected, e.evaluate(null));
    }
    return e;
  }
}
