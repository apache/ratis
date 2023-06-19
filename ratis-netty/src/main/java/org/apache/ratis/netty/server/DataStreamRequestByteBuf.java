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

package org.apache.ratis.netty.server;

import org.apache.ratis.datastream.impl.DataStreamPacketImpl;
import org.apache.ratis.io.WriteOption;
import org.apache.ratis.proto.RaftProtos.DataStreamPacketHeaderProto.Type;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.DataStreamRequest;
import org.apache.ratis.protocol.DataStreamRequestHeader;
import org.apache.ratis.thirdparty.com.google.common.collect.Lists;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.ratis.thirdparty.io.netty.buffer.Unpooled;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Implements {@link DataStreamRequest} with {@link ByteBuf}.
 * <p>
 * This class is immutable.
 */
public class DataStreamRequestByteBuf extends DataStreamPacketImpl implements DataStreamRequest {
  private final AtomicReference<ByteBuf> buf;
  private final List<WriteOption> options;

  public DataStreamRequestByteBuf(ClientId clientId, Type type, long streamId, long streamOffset,
                                  Iterable<WriteOption> options, ByteBuf buf) {
    super(clientId, type, streamId, streamOffset);
    this.buf = new AtomicReference<>(buf != null? buf.asReadOnly(): Unpooled.EMPTY_BUFFER);
    this.options = Collections.unmodifiableList(Lists.newArrayList(options));
  }

  public DataStreamRequestByteBuf(DataStreamRequestHeader header, ByteBuf buf) {
    this(header.getClientId(), header.getType(), header.getStreamId(), header.getStreamOffset(),
         header.getWriteOptionList(), buf);
  }

  ByteBuf getBuf() {
    return Optional.ofNullable(buf.get()).orElseThrow(
        () -> new IllegalStateException("buf is already released in " + this));
  }

  @Override
  public long getDataLength() {
    return getBuf().readableBytes();
  }

  public ByteBuf slice() {
    return getBuf().slice();
  }

  public void release() {
    final ByteBuf got = buf.getAndSet(null);
    if (got != null) {
      got.release();
    }
  }

  @Override
  public List<WriteOption> getWriteOptionList() {
    return options;
  }
}
