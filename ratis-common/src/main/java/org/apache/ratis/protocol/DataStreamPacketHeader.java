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

import org.apache.ratis.datastream.impl.DataStreamPacketImpl;
import org.apache.ratis.proto.RaftProtos.DataStreamPacketHeaderProto.Type;
import org.apache.ratis.util.SizeInBytes;

/** The header format is streamId, streamOffset, dataLength. */
public class DataStreamPacketHeader extends DataStreamPacketImpl {
  private static final SizeInBytes SIZE_OF_HEADER_LEN = SizeInBytes.valueOf(4);
  private static final SizeInBytes SIZE_OF_HEADER_BODY_LEN = SizeInBytes.valueOf(8);

  private final long dataLength;

  public DataStreamPacketHeader(ClientId clientId, Type type, long streamId, long streamOffset, long dataLength) {
    super(clientId, type, streamId, streamOffset);
    this.dataLength = dataLength;
  }

  @Override
  public long getDataLength() {
    return dataLength;
  }

  public static int getSizeOfHeaderLen() {
    return SIZE_OF_HEADER_LEN.getSizeInt();
  }

  public static int getSizeOfHeaderBodyLen() {
    return SIZE_OF_HEADER_BODY_LEN.getSizeInt();
  }
}