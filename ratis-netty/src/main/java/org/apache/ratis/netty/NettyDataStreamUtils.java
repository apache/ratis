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
import org.apache.ratis.protocol.DataStreamReplyHeader;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;

public interface NettyDataStreamUtils {
  static DataStreamReplyByteBuffer decode(ByteBuf buf) {
    final DataStreamReplyHeader header = DataStreamReplyHeader.read(buf::readLong, buf.readableBytes());
    if (header == null) {
      return null;
    }
    final int dataLength = Math.toIntExact(header.getDataLength());

    if (buf.readableBytes() < dataLength) {
      buf.resetReaderIndex();
      return null;
    }

    final ByteBuffer buffer;
    if (dataLength > 0) {
      buffer = buf.slice(buf.readerIndex(), dataLength).nioBuffer();
      buf.readerIndex(buf.readerIndex() + dataLength);
    } else {
      buffer = null;
    }
    buf.markReaderIndex();

    return new DataStreamReplyByteBuffer(header, buffer);
  }
}
