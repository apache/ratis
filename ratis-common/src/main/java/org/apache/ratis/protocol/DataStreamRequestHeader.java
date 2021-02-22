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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.ratis.io.StandardWriteOption;
import org.apache.ratis.io.WriteOption;
import org.apache.ratis.proto.RaftProtos.DataStreamPacketHeaderProto;
import org.apache.ratis.proto.RaftProtos.DataStreamPacketHeaderProto.Type;
import org.apache.ratis.proto.RaftProtos.DataStreamRequestHeaderProto;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The header format is the same {@link DataStreamPacketHeader}
 * since there are no additional fields.
 */
public class DataStreamRequestHeader extends DataStreamPacketHeader implements DataStreamRequest {
  private static final Logger LOG = LoggerFactory.getLogger(DataStreamRequestHeader.class);

  public static DataStreamRequestHeader read(ByteBuf buf) {
    if (getSizeOfHeaderLen() > buf.readableBytes()) {
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

  private final WriteOption[] options;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public DataStreamRequestHeader(ClientId clientId, Type type, long streamId, long streamOffset, long dataLength,
      WriteOption... options) {
    super(clientId, type, streamId, streamOffset, dataLength);
    this.options = options;
  }

  @Override
  @SuppressFBWarnings("EI_EXPOSE_REP")
  public WriteOption[] getWriteOptions() {
    return options;
  }
}