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

import org.apache.ratis.util.SizeInBytes;

import java.util.function.LongSupplier;

/** The header format is {@link DataStreamPacketHeader}, bytesWritten and flags. */
public class DataStreamReplyHeader extends DataStreamPacketHeader implements DataStreamReply {
  private static final SizeInBytes SIZE = SizeInBytes.valueOf(DataStreamPacketHeader.getSize() + 16);

  public static int getSize() {
    return SIZE.getSizeInt();
  }

  public static DataStreamReplyHeader read(LongSupplier readLong, int readableBytes) {
    if (readableBytes < getSize()) {
      return null;
    }
    final DataStreamPacketHeader packerHeader = DataStreamPacketHeader.read(readLong, readableBytes);
    if (packerHeader == null) {
      return null;
    }
    return new DataStreamReplyHeader(packerHeader, readLong.getAsLong(),
        DataStreamReply.getSuccess(readLong.getAsLong()));
  }

  private final long bytesWritten;
  private final boolean success;

  public DataStreamReplyHeader(long streamId, long streamOffset, long dataLength, long bytesWritten, boolean success) {
    super(streamId, streamOffset, dataLength);
    this.bytesWritten = bytesWritten;
    this.success = success;
  }

  public DataStreamReplyHeader(DataStreamPacketHeader header, long bytesWritten, boolean success) {
    this(header.getStreamId(), header.getStreamOffset(), header.getDataLength(), bytesWritten, success);
  }

  @Override
  public long getBytesWritten() {
    return bytesWritten;
  }

  @Override
  public boolean isSuccess() {
    return success;
  }
}