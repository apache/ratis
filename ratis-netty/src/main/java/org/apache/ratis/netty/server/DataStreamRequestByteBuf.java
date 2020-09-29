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

import org.apache.ratis.protocol.DataStreamRequest;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;

class DataStreamRequestByteBuf implements DataStreamRequest {
  private long streamId;
  private long dataOffset;
  private ByteBuf buf;

  DataStreamRequestByteBuf(long streamId, long dataOffset, ByteBuf buf) {
    this.streamId = streamId;
    this.dataOffset = dataOffset;
    this.buf = buf;
  }

  @Override
  public long getStreamId() {
    return streamId;
  }

  @Override
  public long getDataOffset() {
    return dataOffset;
  }

  @Override
  public long getDataLength() {
    return buf.capacity();
  }

  public ByteBuf getBuf() {
    return buf;
  }
}
