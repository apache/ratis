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
package org.apache.ratis.datastream.impl;

import org.apache.ratis.proto.RaftProtos.DataStreamPacketHeaderProto.Type;
import java.nio.ByteBuffer;

/**
 * Implements {@link org.apache.ratis.protocol.DataStreamPacket} with {@link ByteBuffer}.
 */
public abstract class DataStreamPacketByteBuffer extends DataStreamPacketImpl {
  private static final ByteBuffer EMPTY = ByteBuffer.allocateDirect(0).asReadOnlyBuffer();

  private final ByteBuffer buffer;

  public DataStreamPacketByteBuffer(long streamId, long streamOffset, ByteBuffer buffer, Type type) {
    super(streamId, streamOffset, type);
    this.buffer = buffer != null? buffer.asReadOnlyBuffer(): EMPTY;
  }

  @Override
  public long getDataLength() {
    return buffer.remaining();
  }

  public ByteBuffer slice() {
    return buffer.slice();
  }
}
