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
package org.apache.ratis.examples.arithmetic.expression;


import org.apache.ratis.BaseTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class TestExpression extends BaseTest {
  @Override
  public int getGlobalTimeoutSeconds() {
    return 1;
  }

  @Test
  public void testArithmeticUtils() throws Exception {
    final Random ran = ThreadLocalRandom.current();
    final byte[] buf = new byte[1024];
    int offset = 0;

    for(int i = 0; i < 10; i++) {
      {
        final int n = ran.nextInt();
        Expression.Utils.int2bytes(n, buf, offset);
        final int m = Expression.Utils.bytes2int(buf, offset);
        Assert.assertEquals(n, m);
        offset += 4;
      }
      {
        final long n = ran.nextLong();
        Expression.Utils.long2bytes(n, buf, offset);
        final long m = Expression.Utils.bytes2long(buf, offset);
        Assert.assertEquals(n, m);
        offset += 8;
      }
      {
        final double n = ran.nextDouble();
        Expression.Utils.double2bytes(n, buf, offset);
        final double m = Expression.Utils.bytes2double(buf, offset);
        Assert.assertTrue(n == m);
        offset += 8;
      }
    }
  }
  @Test
  public void testOp() throws Exception {
    for(BinaryExpression.Op op : BinaryExpression.Op.values()) {
      final byte b = op.byteValue();
      Assert.assertEquals(op, BinaryExpression.Op.valueOf(b));
    }
    for(UnaryExpression.Op op : UnaryExpression.Op.values()) {
      final byte b = op.byteValue();
      Assert.assertEquals(op, UnaryExpression.Op.valueOf(b));
    }
  }

  @Test
  public void testExpression() throws Exception {
    final byte[] buf = new byte[1024];
    int offset = 0;

    {
      final Variable a = new Variable("pi");
      LOG.info("var a: " + a);
      final int len = a.toBytes(buf, offset);
      final Variable a2 = new Variable(buf, offset);
      LOG.info("var a2: " + a2);
      Assert.assertEquals(a.getName(), a2.getName());
      Assert.assertEquals(len, a.length());
      Assert.assertEquals(len, a2.length());
      offset += len;
    }

    {
      final DoubleValue three = new DoubleValue(3);
      LOG.info("double three: " + three.evaluate(null));
      final int len = three.toBytes(buf, offset);
      final DoubleValue three2 = new DoubleValue(buf, offset);
      LOG.info("double three2: " + three2.evaluate(null));
      Assert.assertTrue(three.evaluate(null).equals(three2.evaluate(null)));
      Assert.assertEquals(len, three.length());
      Assert.assertEquals(len, three2.length());
    }
  }
}
