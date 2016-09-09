package org.apache.raft.examples.arithmatic.expression;

import com.google.common.base.Preconditions;
import org.apache.raft.examples.arithmatic.Evaluable;

public interface Expression extends Evaluable {
  enum Type {
    NULL, VARIABLE, DOUBLE, BINARY, UNARY;

    byte byteValue() {
      return (byte) ordinal();
    }

    private static final Type[] VALUES = Type.values();

    static Type valueOf(byte b) {
      Preconditions.checkArgument(b >= 0);
      Preconditions.checkArgument(b < VALUES.length);
      return VALUES[b];
    }
  }

  int toBytes(byte[] buf, int offset);

  int length();

  class Utils {
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

