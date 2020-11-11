package org.apache.ratis;

import java.nio.ByteBuffer;
import org.junit.Assert;

public class DataStreamTestUtils {
    public static final int MODULUS = 23;

    public static byte pos2byte(int pos) {
        return (byte) ('A' + pos%MODULUS);
    }

    public static ByteBuffer initBuffer(int offset, int size) {
        final ByteBuffer buffer = ByteBuffer.allocateDirect(size);
        final int length = buffer.capacity();
        buffer.position(0).limit(length);
        for (int j = 0; j < length; j++) {
            buffer.put(pos2byte(offset + j));
        }
        buffer.flip();
        Assert.assertEquals(length, buffer.remaining());
        return buffer;
    }
}
