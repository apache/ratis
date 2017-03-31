/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.ratis.examples.arithmetic.expression;

import static org.apache.ratis.util.ProtoUtils.toByteString;

import org.apache.ratis.examples.arithmetic.Evaluable;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.util.Preconditions;

public interface Expression extends Evaluable {
  enum Type {
    NULL, VARIABLE, DOUBLE, BINARY, UNARY;

    byte byteValue() {
      return (byte) ordinal();
    }

    private static final Type[] VALUES = Type.values();

    static Type valueOf(byte b) {
      Preconditions.assertTrue(b >= 0);
      Preconditions.assertTrue(b < VALUES.length);
      return VALUES[b];
    }
  }

  int toBytes(byte[] buf, int offset);

  int length();

  class Utils {
    public static Message toMessage(final Expression e) {
      return () -> {
        final byte[] buf = new byte[e.length()];
        final int length = e.toBytes(buf, 0);
        Preconditions.assertTrue(length == buf.length);
        return toByteString(buf);
      };
    }

    public static Expression double2Expression(Double d) {
      return d == null? NullValue.getInstance(): new DoubleValue(d);
    }

    public static Expression bytes2Expression(byte[] buf, int offset) {
      final Type type = Type.valueOf(buf[offset]);
      switch(type) {
        case NULL: return NullValue.getInstance();
        case DOUBLE: return new DoubleValue(buf, offset);
        case VARIABLE: return new Variable(buf, offset);
        case BINARY: return new BinaryExpression(buf, offset);
        case UNARY: return new UnaryExpression(buf, offset);
        default:
          throw new AssertionError("Unknown expression type " + type);
      }
    }

    public static int int2bytes(int v, byte[] buf, int offset) {
      buf[offset    ] = (byte) (v >>> 24);
      buf[offset + 1] = (byte) (v >>> 16);
      buf[offset + 2] = (byte) (v >>> 8);
      buf[offset + 3] = (byte) (v);
      return 4;
    }

    public static int long2bytes(long v, byte[] buf, int offset) {
      int2bytes((int)(v >>> 32), buf, offset);
      int2bytes((int) v        , buf, offset + 4);
      return 8;
    }

    public static int double2bytes(double d, byte[] buf, int offset) {
      final long v = Double.doubleToRawLongBits(d);
      return long2bytes(v, buf, offset);
    }

    public static int bytes2int(byte[] buf, int offset) {
      return ((int) buf[offset] << 24)
          + ((0xFF & buf[offset + 1]) << 16)
          + ((0xFF & buf[offset + 2]) <<  8)
          +  (0xFF & buf[offset + 3]);
    }

    public static long bytes2long(byte[] buf, int offset) {
      return ((long)bytes2int(buf, offset) << 32)
          + (0xFFFFFFFFL & bytes2int(buf, offset + 4));
    }

    public static double bytes2double(byte[] buf, int offset) {
      final long v = bytes2long(buf, offset);
      return Double.longBitsToDouble(v);
    }
  }
}

