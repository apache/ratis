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

import org.apache.ratis.util.Preconditions;

import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.DoubleFunction;
import java.util.function.UnaryOperator;

public class UnaryExpression implements Expression {
  static final BiFunction<Op, Expression, String> PREFIX_OP_TO_STRING = (op, e) -> op + "" + e;
  static final BiFunction<Op, Expression, String> POSTFIX_OP_TO_STRING = (op, e) -> e + "" + op;

  public enum Op implements UnaryOperator<Expression>, DoubleFunction<Expression> {
    NEG("~"), SQRT("√"), SQUARE("^2", POSTFIX_OP_TO_STRING);

    final String symbol;
    final BiFunction<Op, Expression, String> stringFunction;

    Op(String symbol) {
      this(symbol, PREFIX_OP_TO_STRING);
    }
    Op(String symbol, BiFunction<Op, Expression, String> stringFunction) {
      this.symbol = symbol;
      this.stringFunction = stringFunction;
    }

    byte byteValue() {
      return (byte) ordinal();
    }

    @Override
    public Expression apply(Expression e) {
      return new UnaryExpression(this, e);
    }

    @Override
    public Expression apply(double value) {
      return new UnaryExpression(this, new DoubleValue(value));
    }

    @Override
    public String toString() {
      return symbol;
    }

    public String toString(Expression e) {
      return stringFunction.apply(this, e);
    }

    static final Op[] VALUES = Op.values();

    static Op valueOf(byte b) {
      Preconditions.assertTrue(b < VALUES.length);
      return VALUES[b];
    }

    public String getSymbol() {
      return symbol;
    }
  }

  final Op op;
  final Expression expression;

  UnaryExpression(byte[] buf, int offset) {
    Preconditions.assertTrue(buf[offset] == Type.UNARY.byteValue());
    op = Op.valueOf(buf[offset + 1]);
    expression = Utils.bytes2Expression(buf, offset + 2);
  }

  public UnaryExpression(Op op, Expression expression) {
    this.op = op;
    this.expression = expression;
  }

  @Override
  public int toBytes(byte[] buf, int offset) {
    buf[offset] = Type.UNARY.byteValue();
    buf[offset + 1] = op.byteValue();
    final int length = expression.toBytes(buf, offset + 2);
    return 2 + length;
  }

  @Override
  public int length() {
    return 2 + expression.length();
  }

  @Override
  public Double evaluate(Map<String, Double> variableMap) {
    final double value = expression.evaluate(variableMap);
    switch (op) {
      case NEG:
        return -value;
      case SQRT:
        return Math.sqrt(value);
      case SQUARE:
        return value * value;
      default:
        throw new AssertionError("Unexpected op value: " + op);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    UnaryExpression that = (UnaryExpression) o;
    return op == that.op &&
        Objects.equals(expression, that.expression);
  }

  @Override
  public int hashCode() {
    return Objects.hash(expression);
  }

  @Override
  public String toString() {
    return op.toString(expression);
  }
}
