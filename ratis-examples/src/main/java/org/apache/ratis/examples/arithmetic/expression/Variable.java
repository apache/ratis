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

import java.util.Map;
import java.util.regex.Pattern;

import org.apache.ratis.examples.arithmetic.AssignmentMessage;
import org.apache.ratis.util.Preconditions;

public class Variable implements Expression {
  static final int LENGTH_LIMIT = 32;
  static final String REGEX = "[a-zA-Z]\\w*";
  static final Pattern PATTERN = Pattern.compile(REGEX);

  static byte[] string2bytes(String s) {
    final byte[] stringBytes = s.getBytes(AssignmentMessage.UTF8);
    final byte[] bytes = new byte[stringBytes.length + 2];
    bytes[0] = Type.VARIABLE.byteValue();
    bytes[1] = (byte)stringBytes.length;
    System.arraycopy(stringBytes, 0, bytes, 2, stringBytes.length);
    return bytes;
  }

  static String extractString(byte[] buf, int offset) {
    Preconditions.assertTrue(buf[offset] == Type.VARIABLE.byteValue());
    final int length = buf[offset + 1];
    final byte[] stringBytes = new byte[length];
    System.arraycopy(buf, offset + 2, stringBytes, 0, length);
    return new String(stringBytes, AssignmentMessage.UTF8);
  }

  static byte[] copyBytes(byte[] buf, int offset) {
    Preconditions.assertTrue(buf[offset] == Type.VARIABLE.byteValue());
    final int length = buf[offset + 1];
    final byte[] copy = new byte[length + 2];
    System.arraycopy(buf, offset, copy, 0, copy.length);
    return copy;
  }

  private final String name;
  private final byte[] encoded;

  private Variable(String name, byte[] encoded) {
    this.name = name;
    this.encoded = encoded;

    if (!PATTERN.matcher(name).matches()) {
      throw new IllegalArgumentException("The variable name \"" + name
          + "\" does not match the pattern \"" + PATTERN + "\"");
    }
    if (encoded.length - 2 > LENGTH_LIMIT) {
      throw new IllegalArgumentException("The variable name \"" + name
          + "\" is longer than the limit = " + LENGTH_LIMIT);
    }
  }

  public Variable(byte[] buf, int offset) {
    this(extractString(buf, offset), copyBytes(buf, offset));
  }

  public Variable(String name) {
    this(name, string2bytes(name));
  }

  public String getName() {
    return name;
  }

  @Override
  public int toBytes(byte[] buf, int offset) {
    System.arraycopy(encoded, 0, buf, offset, encoded.length);
    return encoded.length;
  }

  @Override
  public int length() {
    return encoded.length;
  }

  @Override
  public Double evaluate(Map<String, Double> variableMap) {
    final Double value = variableMap.get(name);
    if (value == null) {
      throw new IllegalStateException("Undefined variable \"" + name + "\"");
    }
    return value;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    } else if (obj == null || !(obj instanceof Variable)) {
      return false;
    }
    final Variable that = (Variable)obj;
    return this.getName().equals(that.getName());
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }

  @Override
  public String toString() {
    return name;
  }
}
