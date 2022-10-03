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

import org.apache.ratis.datastream.impl.DataStreamReplyByteBuffer;
import org.apache.ratis.datastream.impl.DataStreamRequestByteBuffer;
import org.apache.ratis.datastream.impl.DataStreamRequestFilePositionCount;
import org.apache.ratis.io.FilePositionCount;
import org.apache.ratis.io.StandardWriteOption;
import org.apache.ratis.io.WriteOption;
import org.apache.ratis.netty.server.DataStreamRequestByteBuf;
import org.apache.ratis.proto.RaftProtos.DataStreamReplyHeaderProto;
import org.apache.ratis.proto.RaftProtos.DataStreamRequestHeaderProto;
import org.apache.ratis.proto.RaftProtos.DataStreamPacketHeaderProto;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.DataStreamPacketHeader;
import org.apache.ratis.protocol.DataStreamReplyHeader;
import org.apache.ratis.protocol.DataStreamRequest;
import org.apache.ratis.protocol.DataStreamRequestHeader;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBufAllocator;
import org.apache.ratis.thirdparty.io.netty.buffer.Unpooled;
import org.apache.ratis.thirdparty.io.netty.channel.DefaultFileRegion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

public interface NettyDataStreamUtils {
  Logger LOG = LoggerFactory.getLogger(NettyDataStreamUtils.class);

  static ByteBuffer getDataStreamRequestHeaderProtoByteBuffer(DataStreamRequest request) {
    DataStreamPacketHeaderProto.Builder b = DataStreamPacketHeaderProto
        .newBuilder()
        .setClientId(request.getClientId().toByteString())
        .setStreamId(request.getStreamId())
        .setStreamOffset(request.getStreamOffset())
        .setType(request.getType())
        .setDataLength(request.getDataLength());
    for (WriteOption option : request.getWriteOptions()) {
      b.addOptions(DataStreamPacketHeaderProto.Option.forNumber(
          ((StandardWriteOption) option).ordinal()));
    }
    return DataStreamRequestHeaderProto
        .newBuilder()
        .setPacketHeader(b)
        .build()
        .toByteString()
        .asReadOnlyByteBuffer();
  }

  static ByteBuffer getDataStreamReplyHeaderProtoByteBuf(DataStreamReplyByteBuffer reply) {
    DataStreamPacketHeaderProto.Builder b = DataStreamPacketHeaderProto
        .newBuilder()
        .setClientId(reply.getClientId().toByteString())
        .setStreamId(reply.getStreamId())
        .setStreamOffset(reply.getStreamOffset())
        .setType(reply.getType())
        .setDataLength(reply.getDataLength());
    return DataStreamReplyHeaderProto
        .newBuilder()
        .setPacketHeader(b)
        .setBytesWritten(reply.getBytesWritten())
        .setSuccess(reply.isSuccess())
        .addAllCommitInfos(reply.getCommitInfos())
        .build()
        .toByteString()
        .asReadOnlyByteBuffer();
  }

  static void encodeDataStreamRequestHeader(DataStreamRequest request, Consumer<Object> out,
      ByteBufAllocator allocator) {
    final ByteBuffer headerBuf = getDataStreamRequestHeaderProtoByteBuffer(request);

    final ByteBuf headerBodyLenBuf = allocator.directBuffer(DataStreamPacketHeader.getSizeOfHeaderBodyLen());
    headerBodyLenBuf.writeLong(headerBuf.remaining() + request.getDataLength());
    out.accept(headerBodyLenBuf);

    final ByteBuf headerLenBuf = allocator.directBuffer(DataStreamPacketHeader.getSizeOfHeaderLen());
    headerLenBuf.writeInt(headerBuf.remaining());
    out.accept(headerLenBuf);

    out.accept(Unpooled.wrappedBuffer(headerBuf));
  }

  static void encodeDataStreamRequestByteBuffer(DataStreamRequestByteBuffer request, Consumer<Object> out,
      ByteBufAllocator allocator) {
    encodeDataStreamRequestHeader(request, out, allocator);
    out.accept(Unpooled.wrappedBuffer(request.slice()));
  }

  static void encodeDataStreamRequestFilePositionCount(
      DataStreamRequestFilePositionCount request, Consumer<Object> out, ByteBufAllocator allocator) {
    encodeDataStreamRequestHeader(request, out, allocator);
    final FilePositionCount f = request.getFile();
    out.accept(new DefaultFileRegion(f.getFile(), f.getPosition(), f.getCount()));
  }

  static void encodeDataStreamReplyByteBuffer(DataStreamReplyByteBuffer reply, Consumer<ByteBuf> out,
      ByteBufAllocator allocator) {
    ByteBuffer headerBuf = getDataStreamReplyHeaderProtoByteBuf(reply);
    final ByteBuf headerLenBuf = allocator.directBuffer(DataStreamPacketHeader.getSizeOfHeaderLen());
    headerLenBuf.writeInt(headerBuf.remaining());
    out.accept(headerLenBuf);
    out.accept(Unpooled.wrappedBuffer(headerBuf));
    out.accept(Unpooled.wrappedBuffer(reply.slice()));
  }

  static DataStreamRequestByteBuf decodeDataStreamRequestByteBuf(ByteBuf buf) {
    return Optional.ofNullable(decodeDataStreamRequestHeader(buf))
        .map(header -> checkHeader(header, buf))
        .map(header -> new DataStreamRequestByteBuf(header, decodeData(buf, header, ByteBuf::retain)))
        .orElse(null);
  }

  static DataStreamRequestHeader decodeDataStreamRequestHeader(ByteBuf buf) {
    if (DataStreamPacketHeader.getSizeOfHeaderBodyLen() > buf.readableBytes()) {
      return null;
    }

    long headerBodyBufLen = buf.readLong();
    if (headerBodyBufLen > buf.readableBytes()) {
      buf.resetReaderIndex();
      return null;
    }

    int headerBufLen = buf.readInt();
    if (headerBufLen > buf.readableBytes()) {
      buf.resetReaderIndex();
      return null;
    }

    try {
      ByteBuf headerBuf = buf.slice(buf.readerIndex(), headerBufLen);
      DataStreamRequestHeaderProto header = DataStreamRequestHeaderProto.parseFrom(headerBuf.nioBuffer());

      final DataStreamPacketHeaderProto h = header.getPacketHeader();
      if (h.getDataLength() + headerBufLen <= buf.readableBytes()) {
        buf.readerIndex(buf.readerIndex() + headerBufLen);
        WriteOption[] options = new WriteOption[h.getOptionsCount()];
        for (int i = 0; i < options.length; i++) {
          options[i] = StandardWriteOption.values()[h.getOptions(i).ordinal()];
        }

        return new DataStreamRequestHeader(ClientId.valueOf(h.getClientId()), h.getType(), h.getStreamId(),
            h.getStreamOffset(), h.getDataLength(), options);
      } else {
        buf.resetReaderIndex();
        return null;
      }
    } catch (InvalidProtocolBufferException e) {
      LOG.error("Fail to decode request header:", e);
      buf.resetReaderIndex();
      return null;
    }
  }

  static ByteBuffer copy(ByteBuf buf) {
    final byte[] bytes = new byte[buf.readableBytes()];
    buf.readBytes(bytes);
    return ByteBuffer.wrap(bytes);
  }

  static DataStreamReplyByteBuffer decodeDataStreamReplyByteBuffer(ByteBuf buf) {
    return Optional.ofNullable(decodeDataStreamReplyHeader(buf))
        .map(header -> checkHeader(header, buf))
        .map(header -> DataStreamReplyByteBuffer.newBuilder()
            .setDataStreamReplyHeader(header)
            .setBuffer(decodeData(buf, header, NettyDataStreamUtils::copy))
            .build())
        .orElse(null);
  }

  static DataStreamReplyHeader decodeDataStreamReplyHeader(ByteBuf buf) {
    if (DataStreamPacketHeader.getSizeOfHeaderLen() > buf.readableBytes()) {
      return null;
    }

    int headerBufLen = buf.readInt();
    if (headerBufLen > buf.readableBytes()) {
      buf.resetReaderIndex();
      return null;
    }

    try {
      ByteBuf headerBuf = buf.slice(buf.readerIndex(), headerBufLen);
      DataStreamReplyHeaderProto header = DataStreamReplyHeaderProto.parseFrom(headerBuf.nioBuffer());

      final DataStreamPacketHeaderProto h = header.getPacketHeader();
      if (header.getPacketHeader().getDataLength() + headerBufLen <= buf.readableBytes()) {
        buf.readerIndex(buf.readerIndex() + headerBufLen);
        return new DataStreamReplyHeader(ClientId.valueOf(h.getClientId()), h.getType(), h.getStreamId(),
            h.getStreamOffset(), h.getDataLength(), header.getBytesWritten(), header.getSuccess(),
            header.getCommitInfosList());
      } else {
        buf.resetReaderIndex();
        return null;
      }
    } catch (InvalidProtocolBufferException e) {
      LOG.error("Fail to decode reply header:", e);
      buf.resetReaderIndex();
      return null;
    }
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
