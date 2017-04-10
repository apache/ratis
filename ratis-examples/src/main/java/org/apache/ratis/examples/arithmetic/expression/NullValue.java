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

public class NullValue implements Expression {
  private static final NullValue INSTANCE = new NullValue();

  public static NullValue getInstance() {
    return INSTANCE;
  }

  private NullValue() {
  }

  @Override
  public int toBytes(byte[] buf, int offset) {
    Preconditions.assertTrue(offset + length() <= buf.length);
    buf[offset++] = Type.NULL.byteValue();
    return length();
  }

  @Override
  public int length() {
    return 1;
  }

  @Override
  public Double evaluate(Map<String, Double> variableMap) {
    return null;
  }

  @Override
  public String toString() {
    return "null";
  }
}
