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

public class BinaryExpression implements Expression {
  public enum Op {
    ADD("+"), SUBTRACT("-"), MULT("*"), DIV("/");

    final String symbol;

    Op(String symbol) {
      this.symbol = symbol;
    }

    byte byteValue() {
      return (byte) ordinal();
    }

    @Override
    public String toString() {
      return symbol;
    }

    static final Op[] VALUES = Op.values();

    static Op valueOf(byte b) {
      Preconditions.assertTrue(b < VALUES.length);
      return VALUES[b];
    }
  }

  private final Op op;
  private final Expression left, right;

  BinaryExpression(byte[] buf, final int offset) {
    Preconditions.assertTrue(buf[offset] == Type.BINARY.byteValue());
    op = Op.valueOf(buf[offset + 1]);
    left = Utils.bytes2Expression(buf, offset + 2);
    right = Utils.bytes2Expression(buf, offset + 2 + left.length());
  }

  public BinaryExpression(Op op, Expression left, Expression right) {
    this.op = op;
    this.left = left;
    this.right = right;
  }

  @Override
  public int toBytes(byte[] buf, final int offset) {
    buf[offset] = Type.BINARY.byteValue();
    buf[offset + 1] = op.byteValue();
    final int l = left.toBytes(buf, offset + 2);
    final int r = right.toBytes(buf, offset + 2 + l);
    return 2 + l + r;
  }

  @Override
  public int length() {
    return 2 + left.length() + right.length();
  }

  @Override
  public Double evaluate(Map<String, Double> variableMap) {
    final double l = left.evaluate(variableMap);
    final double r = right.evaluate(variableMap);
    switch (op) {
      case ADD:
        return l + r;
      case SUBTRACT:
        return l - r;
      case MULT:
        return l * r;
      case DIV:
        return l / r;
      default:
        throw new AssertionError("Unexpected op value: " + op);
    }
  }

  @Override
  public String toString() {
    return "(" + left + " " + op + " " + right + ")";
  }
}
