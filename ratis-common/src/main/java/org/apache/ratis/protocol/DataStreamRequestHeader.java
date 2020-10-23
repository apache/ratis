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

/**
 * The header format is the same {@link DataStreamPacketHeader}
 * since there are no additional fields.
 */
public class DataStreamRequestHeader extends DataStreamPacketHeader implements DataStreamRequest {
  private static final SizeInBytes SIZE = SizeInBytes.valueOf(DataStreamPacketHeader.getSize());

  public static int getSize() {
    return SIZE.getSizeInt();
  }

  public static DataStreamRequestHeader read(LongSupplier readLong, int readableBytes) {
    if (readableBytes < getSize()) {
      return null;
    }
    final DataStreamPacketHeader packerHeader = DataStreamPacketHeader.read(readLong, readableBytes);
    if (packerHeader == null) {
      return null;
    }
    return new DataStreamRequestHeader(packerHeader);
  }

  public DataStreamRequestHeader(long streamId, long streamOffset, long dataLength) {
    super(streamId, streamOffset, dataLength);
  }

  public DataStreamRequestHeader(DataStreamPacketHeader header) {
    this(header.getStreamId(), header.getStreamOffset(), header.getDataLength());
  }
}