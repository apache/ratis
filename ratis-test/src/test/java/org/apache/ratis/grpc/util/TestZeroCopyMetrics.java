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
package org.apache.ratis.grpc.util;

import org.apache.ratis.grpc.metrics.ZeroCopyMetrics;
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotRequestProto;
import org.apache.ratis.proto.RaftProtos.RaftClientRequestProto;
import org.apache.ratis.test.proto.BinaryRequest;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.io.grpc.Detachable;
import org.apache.ratis.thirdparty.io.grpc.HasByteBuffer;
import org.apache.ratis.thirdparty.io.grpc.KnownLength;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestZeroCopyMetrics {
  @Test
  public void testZeroCopyMetricsTrackMessageTypesAndMarshallerStats() {
    final ZeroCopyMetrics metrics = newZeroCopyMetrics();
    try {
      metrics.onZeroCopyAppendEntries(AppendEntriesRequestProto.getDefaultInstance());
      metrics.onZeroCopyInstallSnapshot(InstallSnapshotRequestProto.getDefaultInstance());
      metrics.onZeroCopyClientRequest(RaftClientRequestProto.getDefaultInstance());
      metrics.onNonZeroCopyMessage(AppendEntriesRequestProto.getDefaultInstance());
      metrics.onReleasedMessage(AppendEntriesRequestProto.getDefaultInstance());

      final ZeroCopyMetrics.ZeroCopyMessageMarshallerMetrics marshallerMetrics = metrics.newMarshallerMetrics();
      marshallerMetrics.onZeroCopyParse(123L, 456L);
      marshallerMetrics.onFallbackNotKnownLength();
      marshallerMetrics.onFallbackNotDetachable();
      marshallerMetrics.onFallbackNotByteBuffer();

      assertEquals(3L, metrics.zeroCopyMessages());
      assertEquals(1L, metrics.nonZeroCopyMessages());
      assertEquals(1L, metrics.releasedMessages());
      assertCounter(metrics, "num_zero_copy_append_entries", 1L);
      assertCounter(metrics, "num_zero_copy_install_snapshot", 1L);
      assertCounter(metrics, "num_zero_copy_client_request", 1L);
      assertCounter(metrics, "bytes_saved_by_zero_copy", 123L);
      assertCounter(metrics, "zero_copy_parse_time_nanos", 456L);
      assertCounter(metrics, "zero_copy_fallback_not_known_length", 1L);
      assertCounter(metrics, "zero_copy_fallback_not_detachable", 1L);
      assertCounter(metrics, "zero_copy_fallback_not_byte_buffer", 1L);
    } finally {
      metrics.unregister();
    }
  }

  @Test
  public void testMarshallerReportsZeroCopyParseMetrics() {
    final BinaryRequest request = BinaryRequest.newBuilder()
        .setData(ByteString.copyFromUtf8("zero-copy"))
        .build();
    final byte[] bytes = request.toByteArray();
    final RecordingMetrics metrics = new RecordingMetrics();
    final AtomicInteger zeroCopyCount = new AtomicInteger();
    final AtomicInteger nonZeroCopyCount = new AtomicInteger();
    final AtomicInteger releasedCount = new AtomicInteger();
    final ZeroCopyMessageMarshaller<BinaryRequest> marshaller = new ZeroCopyMessageMarshaller<>(
        BinaryRequest.getDefaultInstance(),
        ignored -> zeroCopyCount.incrementAndGet(),
        ignored -> nonZeroCopyCount.incrementAndGet(),
        ignored -> releasedCount.incrementAndGet(),
        metrics);

    final BinaryRequest parsed = marshaller.parse(new DetachableByteBufferInputStream(bytes));
    assertEquals(request, parsed);
    assertEquals(1, zeroCopyCount.get());
    assertEquals(0, nonZeroCopyCount.get());
    assertEquals(bytes.length, metrics.bytesSavedByZeroCopy);
    assertTrue(metrics.zeroCopyParseTimeNanos > 0);
    assertEquals(1, marshaller.getUnclosedCount());

    marshaller.release(parsed);
    assertEquals(1, releasedCount.get());
    assertEquals(0, marshaller.getUnclosedCount());
  }

  @Test
  public void testMarshallerReportsFallbackNotKnownLength() {
    final BinaryRequest request = BinaryRequest.newBuilder()
        .setData(ByteString.copyFromUtf8("known-length-fallback"))
        .build();
    final RecordingMetrics metrics = new RecordingMetrics();
    final AtomicInteger nonZeroCopyCount = new AtomicInteger();
    final ZeroCopyMessageMarshaller<BinaryRequest> marshaller = new ZeroCopyMessageMarshaller<>(
        BinaryRequest.getDefaultInstance(),
        ignored -> fail("Should not use zero-copy path"),
        ignored -> nonZeroCopyCount.incrementAndGet(),
        ignored -> { },
        metrics);

    final BinaryRequest parsed = marshaller.parse(new ByteArrayInputStream(request.toByteArray()));
    assertEquals(request, parsed);
    assertEquals(1, nonZeroCopyCount.get());
    assertEquals(1, metrics.fallbackNotKnownLength);
    assertEquals(0, metrics.fallbackNotDetachable);
    assertEquals(0, metrics.fallbackNotByteBuffer);
  }

  @Test
  public void testMarshallerReportsFallbackNotDetachable() {
    final BinaryRequest request = BinaryRequest.newBuilder()
        .setData(ByteString.copyFromUtf8("not-detachable"))
        .build();
    final RecordingMetrics metrics = new RecordingMetrics();
    final AtomicInteger nonZeroCopyCount = new AtomicInteger();
    final ZeroCopyMessageMarshaller<BinaryRequest> marshaller = new ZeroCopyMessageMarshaller<>(
        BinaryRequest.getDefaultInstance(),
        ignored -> fail("Should not use zero-copy path"),
        ignored -> nonZeroCopyCount.incrementAndGet(),
        ignored -> { },
        metrics);

    final BinaryRequest parsed = marshaller.parse(new KnownLengthByteArrayInputStream(request.toByteArray()));
    assertEquals(request, parsed);
    assertEquals(1, nonZeroCopyCount.get());
    assertEquals(0, metrics.fallbackNotKnownLength);
    assertEquals(1, metrics.fallbackNotDetachable);
    assertEquals(0, metrics.fallbackNotByteBuffer);
  }

  @Test
  public void testMarshallerReportsFallbackNotByteBuffer() {
    final BinaryRequest request = BinaryRequest.newBuilder()
        .setData(ByteString.copyFromUtf8("not-byte-buffer"))
        .build();
    final RecordingMetrics metrics = new RecordingMetrics();
    final AtomicInteger nonZeroCopyCount = new AtomicInteger();
    final ZeroCopyMessageMarshaller<BinaryRequest> marshaller = new ZeroCopyMessageMarshaller<>(
        BinaryRequest.getDefaultInstance(),
        ignored -> fail("Should not use zero-copy path"),
        ignored -> nonZeroCopyCount.incrementAndGet(),
        ignored -> { },
        metrics);

    final BinaryRequest parsed = marshaller.parse(new KnownLengthDetachableByteArrayInputStream(request.toByteArray()));
    assertEquals(request, parsed);
    assertEquals(1, nonZeroCopyCount.get());
    assertEquals(0, metrics.fallbackNotKnownLength);
    assertEquals(0, metrics.fallbackNotDetachable);
    assertEquals(1, metrics.fallbackNotByteBuffer);
  }

  private static void assertCounter(ZeroCopyMetrics metrics, String name, long expected) {
    assertEquals(expected, metrics.getRegistry().counter(name).getCount(), name);
  }

  private static ZeroCopyMetrics newZeroCopyMetrics() {
    final ZeroCopyMetrics metrics = new ZeroCopyMetrics();
    metrics.unregister();
    return new ZeroCopyMetrics();
  }

  private static class RecordingMetrics implements ZeroCopyMessageMarshaller.Metrics {
    private long bytesSavedByZeroCopy;
    private long zeroCopyParseTimeNanos;
    private int fallbackNotKnownLength;
    private int fallbackNotDetachable;
    private int fallbackNotByteBuffer;

    @Override
    public void onZeroCopyParse(long bytesSaved, long parseTimeNanos) {
      this.bytesSavedByZeroCopy += bytesSaved;
      this.zeroCopyParseTimeNanos += parseTimeNanos;
    }

    @Override
    public void onFallbackNotKnownLength() {
      fallbackNotKnownLength++;
    }

    @Override
    public void onFallbackNotDetachable() {
      fallbackNotDetachable++;
    }

    @Override
    public void onFallbackNotByteBuffer() {
      fallbackNotByteBuffer++;
    }
  }

  private static class KnownLengthByteArrayInputStream extends ByteArrayInputStream implements KnownLength {
    KnownLengthByteArrayInputStream(byte[] buf) {
      super(buf);
    }
  }

  private static class KnownLengthDetachableByteArrayInputStream extends KnownLengthByteArrayInputStream
      implements Detachable {
    KnownLengthDetachableByteArrayInputStream(byte[] buf) {
      super(buf);
    }

    @Override
    public InputStream detach() {
      return this;
    }
  }

  private static class DetachableByteBufferInputStream extends InputStream
      implements KnownLength, Detachable, HasByteBuffer {
    private final byte[] bytes;
    private int position;
    private int mark;

    DetachableByteBufferInputStream(byte[] bytes) {
      this.bytes = bytes;
    }

    @Override
    public InputStream detach() {
      return this;
    }

    @Override
    public boolean byteBufferSupported() {
      return true;
    }

    @Override
    public ByteBuffer getByteBuffer() {
      return ByteBuffer.wrap(bytes, position, available()).slice();
    }

    @Override
    public int read() {
      return position < bytes.length ? bytes[position++] & 0xff : -1;
    }

    @Override
    public int read(byte[] b, int off, int len) {
      if (position >= bytes.length) {
        return -1;
      }
      final int n = Math.min(len, available());
      System.arraycopy(bytes, position, b, off, n);
      position += n;
      return n;
    }

    @Override
    public long skip(long n) {
      final int skipped = Math.min((int) n, available());
      position += skipped;
      return skipped;
    }

    @Override
    public int available() {
      return bytes.length - position;
    }

    @Override
    public synchronized void mark(int readlimit) {
      this.mark = position;
    }

    @Override
    public synchronized void reset() {
      this.position = mark;
    }

    @Override
    public boolean markSupported() {
      return true;
    }
  }
}
