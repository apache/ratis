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
package org.apache.ratis.protocol;

import java.nio.ByteBuffer;

/**
 * Implements {@link DataStreamPacket} with {@link ByteBuffer}.
 *
 * This class is immutable.
 */
public class DataStreamPacketByteBuffer implements DataStreamPacket {
  private final long streamId;
  private final long streamOffset;
  private final ByteBuffer buffer;

  public DataStreamPacketByteBuffer(long streamId, long streamOffset, ByteBuffer buffer) {
    this.streamId = streamId;
    this.streamOffset = streamOffset;
    this.buffer = buffer.asReadOnlyBuffer();
  }

  @Override
  public long getStreamId() {
    return streamId;
  }

  @Override
  public long getStreamOffset() {
    return streamOffset;
  }

  public ByteBuffer slice() {
    return buffer.slice();
  }

  @Override
  public long getDataLength() {
    return buffer.remaining();
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "{"
        + "streamId=" + getStreamId()
        + ", streamOffset=" + getStreamOffset()
        + ", dataLength=" + getDataLength()
        + '}';
  }
}
