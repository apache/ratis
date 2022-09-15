/*
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
package org.apache.ratis.server.raftlog.segmented;

import org.apache.ratis.BaseTest;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * Test {@link BufferedWriteChannel}
 */
public class TestBufferedWriteChannel extends BaseTest {
  class FakeFileChannel extends FileChannel {
    private long position = 0;
    private long forcedPosition = 0;

    void assertValues(long expectedPosition, long expectedForcedPosition) {
      Assert.assertEquals(expectedPosition, position);
      Assert.assertEquals(expectedForcedPosition, forcedPosition);
    }

    @Override
    public int read(ByteBuffer dst) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int read(ByteBuffer dst, long position) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int write(ByteBuffer src, long position) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int write(ByteBuffer src) {
      final int remaining = src.remaining();
      LOG.info("write {} bytes", remaining);
      position += remaining;
      src.position(src.limit());
      return remaining;
    }

    @Override
    public long position() {
      throw new UnsupportedOperationException();
    }

    @Override
    public FileChannel position(long newPosition) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long size() {
      throw new UnsupportedOperationException();
    }

    @Override
    public FileChannel truncate(long newSize) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void force(boolean metaData) {
      LOG.info("force at position {}", position);
      forcedPosition = position;
    }

    @Override
    public long transferTo(long position, long count, WritableByteChannel target) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long transferFrom(ReadableByteChannel src, long position, long count) {
      throw new UnsupportedOperationException();
    }

    @Override
    public MappedByteBuffer map(MapMode mode, long position, long size) {
      throw new UnsupportedOperationException();
    }

    @Override
    public FileLock lock(long position, long size, boolean shared) {
      throw new UnsupportedOperationException();
    }

    @Override
    public FileLock tryLock(long position, long size, boolean shared) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected void implCloseChannel() {
      throw new UnsupportedOperationException();
    }
  }

  @Test
  public void testFlush() throws Exception {
    final byte[] bytes = new byte[10];
    final ByteBuffer buffer = ByteBuffer.wrap(bytes);
    final FakeFileChannel fake = new FakeFileChannel();
    final BufferedWriteChannel out = new BufferedWriteChannel(fake, buffer);

    // write exactly buffer size, then flush.
    fake.assertValues(0, 0);
    out.write(bytes);
    int pos = bytes.length;
    fake.assertValues(pos,  0);
    out.flush();
    int force = pos;
    fake.assertValues(pos, force);

    {
      // write less than buffer size, then flush.
      int n = bytes.length/2;
      out.write(new byte[n]);
      fake.assertValues(pos, force);
      out.flush();
      pos += n;
      force = pos;
      fake.assertValues(pos, force);
    }

    {
      // write less than buffer size twice, then flush.
      int n = bytes.length*2/3;
      out.write(new byte[n]);
      fake.assertValues(pos, force);
      out.write(new byte[n]);
      fake.assertValues(pos + bytes.length, force);
      out.flush();
      pos += 2*n;
      force = pos;
      fake.assertValues(pos, force);
    }

    {
      // write more than buffer size, then flush.
      int n = bytes.length*3/2;
      out.write(new byte[n]);
      fake.assertValues(pos + bytes.length, force);
      out.flush();
      pos += n;
      force = pos;
      fake.assertValues(pos, force);
    }
  }
}
