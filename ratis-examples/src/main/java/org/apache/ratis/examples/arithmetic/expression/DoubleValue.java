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

public class DoubleValue implements Expression {
  public static final DoubleValue ZERO = new DoubleValue(0);
  public static final DoubleValue ONE = new DoubleValue(1);

  private final double value;

  public DoubleValue(double value) {
    this.value = value;
  }

  DoubleValue(byte[] buf, int offset) {
    this(Utils.bytes2double(buf, offset + 1));
    Preconditions.assertTrue(buf[offset] == Type.DOUBLE.byteValue());
  }

  @Override
  public int toBytes(byte[] buf, int offset) {
    Preconditions.assertTrue(offset + length() <= buf.length);
    buf[offset++] = Type.DOUBLE.byteValue();
    Utils.double2bytes(value, buf, offset);
    return length();
  }

  @Override
  public int length() {
    return 9;
  }

  @Override
  public Double evaluate(Map<String, Double> variableMap) {
    return value;
  }

  @Override
  public String toString() {
    final long n = (long)value;
    return n == value? String.valueOf(n): String.valueOf(value);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DoubleValue that = (DoubleValue) o;
    return Double.compare(that.value, value) == 0;
  }

  @Override
  public int hashCode() {
    return Objects.hash(value);
  }
}
