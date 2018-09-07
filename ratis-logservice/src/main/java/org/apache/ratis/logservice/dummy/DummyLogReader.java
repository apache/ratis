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
package org.apache.ratis.logservice.dummy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.ratis.logservice.api.LogReader;

public class DummyLogReader implements LogReader {
  private static final byte[] IMMUTABLE_BYTES = new byte[0];

  @Override
  public void close() {}

  @Override
  public void seek(long recordId) throws IOException {
    // Noop.
    return;
  }

  @Override
  public ByteBuffer readNext() throws IOException {
    return ByteBuffer.wrap(IMMUTABLE_BYTES);
  }

  @Override
  public List<ByteBuffer> readBulk(int numRecords) throws IOException {
    ArrayList<ByteBuffer> records = new ArrayList<>(numRecords);
    for (int i = 0; i < numRecords; i++) {
      records.add(ByteBuffer.wrap(IMMUTABLE_BYTES));
    }
    return records;
  }

  @Override
  public void readNext(ByteBuffer buffer) throws IOException {
    buffer.clear();
    if (buffer.remaining() < IMMUTABLE_BYTES.length) {
      throw new IllegalArgumentException("Cannot read data into buffer of size " + buffer.remaining());
    }
    buffer.put(IMMUTABLE_BYTES);
    buffer.flip();
  }

  @Override
  public int readBulk(List<ByteBuffer> buffers) throws IOException {
    for (ByteBuffer buffer : buffers) {
      readNext(buffer);
    }
    return buffers.size();
  }

  @Override
  public long getPosition() {
    // Always at the head of the list
    return 0;
  }
}
