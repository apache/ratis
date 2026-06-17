/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.ratis.datastream.impl;

import org.apache.ratis.proto.RaftProtos.DataStreamPacketHeaderProto.Type;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.ratis.thirdparty.io.netty.buffer.Unpooled;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Extends {@link DataStreamPacketImpl} with {@link ByteBuf}.
 * <p>
 * This class is immutable.
 */
class DataStreamPacketByteBuf extends DataStreamPacketImpl {
  private final AtomicReference<ByteBuf> buf;

  DataStreamPacketByteBuf(ClientId clientId, Type type, long streamId, long streamOffset, ByteBuf buf) {
    super(clientId, type, streamId, streamOffset);
    this.buf = new AtomicReference<>(buf != null? buf.asReadOnly(): Unpooled.EMPTY_BUFFER);
  }

  final ByteBuf getBuf() {
    final ByteBuf got = buf.get();
    if (got == null) {
      throw new IllegalStateException("buf is already released in " + this);
    }
    return got;
  }

  @Override
  public final long getDataLength() {
    return getBuf().readableBytes();
  }

  public final ByteBuf slice() {
    return getBuf().slice();
  }

  public final void release() {
    final ByteBuf got = buf.getAndSet(null);
    if (got != null) {
      got.release();
    }
  }
}
