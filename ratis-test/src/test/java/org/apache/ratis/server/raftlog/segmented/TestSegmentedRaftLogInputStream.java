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
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.SizeInBytes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

public class TestSegmentedRaftLogInputStream extends BaseTest {
  @ParameterizedTest
  @ValueSource(strings = {"valid", "partial", "corrupt", "io-failure", "unopened", "reader"})
  public void testReaderOwnership(String mode) throws Exception {
    final ByteBuffer headerBuffer = SegmentedRaftLogFormat.getHeaderBytebuffer().duplicate();
    byte[] data = new byte[headerBuffer.remaining()];
    headerBuffer.get(data);
    if ("partial".equals(mode)) {
      data = Arrays.copyOf(data, data.length / 2);
    } else if ("corrupt".equals(mode)) {
      data[0] ^= 0x7f;
    }
    final AtomicInteger closed = new AtomicInteger();
    final InputStream source = new FilterInputStream(new ByteArrayInputStream(data)) {
      @Override
      public int read(byte[] bytes, int offset, int length) throws IOException {
        if ("io-failure".equals(mode)) {
          throw new IOException("injected header read failure");
        }
        return super.read(bytes, offset, length);
      }

      @Override
      public void close() throws IOException {
        closed.incrementAndGet();
        super.close();
      }
    };
    final File file = new File("test-reader-ownership");
    final SizeInBytes maxOpSize = SizeInBytes.valueOf("1MB");
    try (MockedStatic<FileUtils> files = Mockito.mockStatic(FileUtils.class)) {
      files.when(() -> FileUtils.newInputStream(file)).thenReturn(source);
      if ("reader".equals(mode)) {
        try (SegmentedRaftLogReader ignored = SegmentedRaftLogReader.newInstance(file, maxOpSize, null)) {
          Assertions.assertEquals(0, closed.get());
        }
      } else {
        try (SegmentedRaftLogInputStream in = new SegmentedRaftLogInputStream(
            file, LogSegmentStartEnd.valueOf(0), maxOpSize, null)) {
          if ("corrupt".equals(mode) || "io-failure".equals(mode)) {
            Assertions.assertThrows(IOException.class, in::nextEntry);
            Assertions.assertEquals(1, closed.get());
          } else if (!"unopened".equals(mode)) {
            Assertions.assertNull(in.nextEntry());
            Assertions.assertEquals("partial".equals(mode) ? 1 : 0, closed.get());
          }
        }
      }
      Assertions.assertEquals("unopened".equals(mode) ? 0 : 1, closed.get());
    }
  }
}
