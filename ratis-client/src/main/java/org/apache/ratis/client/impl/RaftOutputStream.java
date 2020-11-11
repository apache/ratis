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
package org.apache.ratis.client.impl;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.ProtoUtils;
import org.apache.ratis.util.SizeInBytes;

import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/** An {@link OutputStream} implementation using {@link org.apache.ratis.client.api.AsyncApi#send(Message)} API. */
public class RaftOutputStream extends OutputStream {
  private final Supplier<RaftClient> client;
  private final AtomicBoolean closed = new AtomicBoolean();
  private final Queue<CompletableFuture<Long>> flushFutures = new LinkedList<>();

  private final byte[] buffer;
  private int byteCount;
  private long byteFlushed;

  public RaftOutputStream(Supplier<RaftClient> clientSupplier, SizeInBytes bufferSize) {
    this.client = JavaUtils.memoize(clientSupplier);
    this.buffer = new byte[bufferSize.getSizeInt()];
  }

  private RaftClient getClient() {
    return client.get();
  }

  @Override
  public void write(int b) throws IOException {
    checkClosed();
    buffer[byteCount++] = (byte)b;
    flushIfNecessary();
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    checkClosed();
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new ArrayIndexOutOfBoundsException();
    }

    for(int total = 0; total < len; ) {
      final int toWrite = Math.min(len - total, buffer.length - byteCount);
      System.arraycopy(b, off + total, buffer, byteCount, toWrite);
      byteCount += toWrite;
      total += toWrite;
      flushIfNecessary();
    }
  }

  private void flushIfNecessary() {
    if (byteCount == buffer.length) {
      flushAsync();
    }
  }

  /** Non-blocking flush call */
  private void flushAsync() {
    final long pos = byteFlushed;
    if (byteCount == 0) {
      return;
    }

    final CompletableFuture<Long> f = getClient().async().send(
        Message.valueOf(ProtoUtils.toByteString(buffer, 0, byteCount))
    ).thenApply(reply -> RaftClientImpl.handleRaftException(reply, CompletionException::new)
    ).thenApply(reply -> reply != null && reply.isSuccess()? pos: null);
    flushFutures.offer(f);

    byteFlushed += byteCount;
    byteCount = 0;
  }

  /** Blocking flush call */
  private void flushImpl() throws IOException {
    final long pos = byteFlushed;
    flushAsync();
    for(; !flushFutures.isEmpty();) {
      final Long flushed = flushFutures.poll().join();
      if (flushed == null) {
        throw new IOException("Failed to flush at position " + pos);
      }
    }
  }

  @Override
  public void flush() throws IOException {
    checkClosed();
    flushImpl();
  }

  private void checkClosed() throws IOException {
    if (closed.get()) {
      throw new IOException(this + " was closed.");
    }
  }

  @Override
  public void close() throws IOException {
    if (closed.compareAndSet(false, true)) {
      flushImpl();
      getClient().close();
    }
  }

  @Override
  public String toString() {
    return JavaUtils.getClassSimpleName(getClass()) + "-" + getClient().getId() + ":byteFlushed=" + byteFlushed;
  }
}
