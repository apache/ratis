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
package org.apache.ratis.examples.arithmetic.cli;

import org.apache.ratis.examples.arithmetic.expression.DoubleValue;
import org.apache.ratis.examples.arithmetic.expression.Variable;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.ratis.examples.arithmetic.expression.BinaryExpression.Op.ADD;
import static org.apache.ratis.examples.arithmetic.expression.BinaryExpression.Op.MULT;
import static org.apache.ratis.examples.arithmetic.expression.BinaryExpression.Op.SUBTRACT;
import static org.apache.ratis.examples.arithmetic.expression.UnaryExpression.Op.MINUS;
import static org.apache.ratis.examples.arithmetic.expression.UnaryExpression.Op.NEG;
import static org.apache.ratis.examples.arithmetic.expression.UnaryExpression.Op.SQRT;

public class TestAssignCli {
  @Test
  public void createExpression() {
    Assert.assertEquals(
        new DoubleValue(2.0),
        new Assign().createExpression("2.0"));

    Assert.assertEquals(
        new DoubleValue(42.0),
        new Assign().createExpression("42"));

    Assert.assertEquals(
        MULT.apply(2.0, new Variable("a")),
        new Assign().createExpression("2*a"));

    Assert.assertEquals(
        MULT.apply(new Variable("v1"), 2.0),
        new Assign().createExpression("v1 * 2"));

    Assert.assertEquals(
        ADD.apply(2.0, 1.0),
        new Assign().createExpression("2+1"));

    Assert.assertEquals(
        SUBTRACT.apply(1.0, 6.0),
        new Assign().createExpression("1 - 6"));

    Assert.assertEquals(
        ADD.apply(new Variable("a"), new Variable("v2")),
        new Assign().createExpression("a+v2"));

    Assert.assertEquals(
        ADD.apply(new Variable("v1"), new Variable("b")),
        new Assign().createExpression("v1 + b"));

    Assert.assertEquals(
        SQRT.apply(new Variable("a")),
        new Assign().createExpression("√a"));

    Assert.assertEquals(
        SQRT.apply(new Variable("ABC")),
        new Assign().createExpression("√ABC"));

    Assert.assertEquals(
        SQRT.apply(2.0),
        new Assign().createExpression("√2"));

    Assert.assertEquals(
        NEG.apply(2.0),
        new Assign().createExpression("~2.0"));

    Assert.assertEquals(
        MINUS.apply(6.0),
        new Assign().createExpression("-6.0"));
  }
}