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
package org.apache.ratis.netty;

import org.apache.ratis.datastream.impl.DataStreamPacketByteBuffer;
import org.apache.ratis.datastream.impl.DataStreamReplyByteBuffer;
import org.apache.ratis.netty.server.DataStreamRequestByteBuf;
import org.apache.ratis.protocol.DataStreamPacketHeader;
import org.apache.ratis.protocol.DataStreamReplyHeader;
import org.apache.ratis.protocol.DataStreamRequestHeader;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.ratis.thirdparty.io.netty.buffer.PooledByteBufAllocator;
import org.apache.ratis.thirdparty.io.netty.buffer.Unpooled;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

public interface NettyDataStreamUtils {
  static void encodeDataStreamPacketByteBuffer(DataStreamPacketByteBuffer packet, Consumer<ByteBuf> out) {
    final ByteBuf buf = PooledByteBufAllocator.DEFAULT.directBuffer(packet.getHeaderSize());
    packet.writeHeaderTo(buf::writeLong);
    out.accept(buf);
    out.accept(Unpooled.wrappedBuffer(packet.slice()));
  }

  static DataStreamRequestByteBuf decodeDataStreamRequestByteBuf(ByteBuf buf) {
    return Optional.ofNullable(DataStreamRequestHeader.read(buf::readLong, buf.readableBytes()))
        .map(header -> checkHeader(header, buf))
        .map(header -> new DataStreamRequestByteBuf(header, decodeData(buf, header, ByteBuf::retain)))
        .orElse(null);
  }

  static DataStreamReplyByteBuffer decodeDataStreamReplyByteBuffer(ByteBuf buf) {
    return Optional.ofNullable(DataStreamReplyHeader.read(buf::readLong, buf.readableBytes()))
        .map(header -> checkHeader(header, buf))
        .map(header -> new DataStreamReplyByteBuffer(header, decodeData(buf, header, ByteBuf::nioBuffer)))
        .orElse(null);
  }

  static <HEADER extends DataStreamPacketHeader> HEADER checkHeader(HEADER header, ByteBuf buf) {
    if (header == null) {
      return null;
    }
    if (buf.readableBytes() < header.getDataLength()) {
      buf.resetReaderIndex();
      return null;
    }
    return header;
  }

  static <DATA> DATA decodeData(ByteBuf buf, DataStreamPacketHeader header, Function<ByteBuf, DATA> toData) {
    final int dataLength = Math.toIntExact(header.getDataLength());
    final DATA data;
    if (dataLength > 0) {
      data = toData.apply(buf.slice(buf.readerIndex(), dataLength));
      buf.readerIndex(buf.readerIndex() + dataLength);
    } else {
      data = null;
    }
    buf.markReaderIndex();
    return data;
  }
}
