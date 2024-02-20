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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test {@link BufferedWriteChannel}
 */
public class TestBufferedWriteChannel extends BaseTest {
  class FakeFileChannel extends FileChannel {
    private long position = 0;
    private long forcedPosition = 0;

    void assertValues(long expectedPosition, long expectedForcedPosition) {
      Assertions.assertEquals(expectedPosition, position);
      Assertions.assertEquals(expectedForcedPosition, forcedPosition);
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
      return remaining;
    }

    @Override
    public long position() {
      return position;
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

  static ByteBuffer allocateByteBuffer(int size) {
    final ByteBuffer buffer = ByteBuffer.allocate(size);
    for(int i = 0; i < size; i++) {
      buffer.put(i, (byte)i);
    }
    return buffer.asReadOnlyBuffer();
  }


  @Test
  public void testWriteToChannel() throws Exception {
    for(int n = 1; n < 1 << 20; n <<=2) {
      runTestWriteToChannel(n - 1);
      runTestWriteToChannel(n);
      runTestWriteToChannel(n + 1);
    }
  }

  void runTestWriteToChannel(int bufferSize) throws Exception {
    final ByteBuffer buffer = allocateByteBuffer(2 * bufferSize);
    final FakeFileChannel fake = new FakeFileChannel();
    final BufferedWriteChannel out = new BufferedWriteChannel("test", fake, ByteBuffer.allocate(0));

    fake.assertValues(0, 0);
    final AtomicInteger pos = new AtomicInteger();
    final AtomicInteger force = new AtomicInteger();

    {
      // write exactly buffer size, then flush.
      writeToChannel(out, fake, pos, force, buffer, bufferSize);
      flush(out, fake, pos, force);
    }

    {
      // write less than buffer size, then flush.
      writeToChannel(out, fake, pos, force, buffer, bufferSize/2);
      flush(out, fake, pos, force);
    }

    {
      // write less than buffer size twice, then flush.
      final int n = bufferSize*2/3;
      writeToChannel(out, fake, pos, force, buffer, n);
      writeToChannel(out, fake, pos, force, buffer, n);
      flush(out, fake, pos, force);
    }

    {
      // write more than buffer size, then flush.
      writeToChannel(out, fake, pos, force, buffer, bufferSize*3/2);
      flush(out, fake, pos, force);
    }
  }

  static void writeToChannel(BufferedWriteChannel out, FakeFileChannel fake, AtomicInteger pos, AtomicInteger force,
      ByteBuffer buffer, int n) throws IOException {
    buffer.position(0).limit(n);
    out.writeToChannel(buffer);
    pos.addAndGet(n);
    fake.assertValues(pos.get(), force.get());
  }

  static void flush(BufferedWriteChannel out, FakeFileChannel fake,
      AtomicInteger pos, AtomicInteger force) throws IOException {
    final int existing = out.writeBufferPosition();
    out.flush();
    Assertions.assertEquals(0, out.writeBufferPosition());
    pos.addAndGet(existing);
    force.set(pos.get());
    fake.assertValues(pos.get(), force.get());
  }

  static void writeToBuffer(BufferedWriteChannel out, FakeFileChannel fake, AtomicInteger pos, AtomicInteger force,
      int bufferCapacity, ByteBuffer buffer, int n) throws IOException {
    final int existing = out.writeBufferPosition();
    buffer.position(0).limit(n);
    out.writeToBuffer(n, b -> b.put(buffer));
    if (existing + n > bufferCapacity) {
      pos.addAndGet(existing);
      Assertions.assertEquals(n, out.writeBufferPosition());
    } else {
      Assertions.assertEquals(existing + n, out.writeBufferPosition());
    }
    fake.assertValues(pos.get(), force.get());
  }

  @Test
  public void testWriteToBuffer() throws Exception {
    for(int n = 1; n < 1 << 20; n <<=2) {
      runTestWriteToBuffer(n - 1);
      runTestWriteToBuffer(n);
      runTestWriteToBuffer(n + 1);
    }
  }

  void runTestWriteToBuffer(int bufferSize) throws Exception {
    final ByteBuffer buffer = allocateByteBuffer(2 * bufferSize);
    final FakeFileChannel fake = new FakeFileChannel();
    final BufferedWriteChannel out = new BufferedWriteChannel("test", fake, ByteBuffer.allocate(bufferSize));

    fake.assertValues(0, 0);
    final AtomicInteger pos = new AtomicInteger();
    final AtomicInteger force = new AtomicInteger();

    {
      // write exactly buffer size, then flush.
      writeToBuffer(out, fake, pos, force, bufferSize, buffer, bufferSize);
      flush(out, fake, pos, force);
    }

    {
      // write less than buffer size, then flush.
      writeToBuffer(out, fake, pos, force, bufferSize, buffer, bufferSize/2);
      flush(out, fake, pos, force);
    }

    {
      // write less than buffer size twice, then flush.
      final int n = bufferSize*2/3;
      writeToBuffer(out, fake, pos, force, bufferSize, buffer, n);
      writeToBuffer(out, fake, pos, force, bufferSize, buffer, n);
      flush(out, fake, pos, force);
    }
  }
}
