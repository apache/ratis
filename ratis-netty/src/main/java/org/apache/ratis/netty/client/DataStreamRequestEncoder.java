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

package org.apache.ratis.netty.client;

import org.apache.ratis.datastream.impl.DataStreamRequestByteBuffer;
import org.apache.ratis.thirdparty.io.netty.buffer.Unpooled;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.ratis.thirdparty.io.netty.handler.codec.MessageToMessageEncoder;

import java.nio.ByteBuffer;
import java.util.List;

public class DataStreamRequestEncoder extends MessageToMessageEncoder<DataStreamRequestByteBuffer> {

  @Override
  protected void encode(ChannelHandlerContext channelHandlerContext,
      DataStreamRequestByteBuffer request, List<Object> list) {
    final ByteBuffer data = request.slice();
    ByteBuffer bb = ByteBuffer.allocateDirect(24);
    bb.putLong(request.getStreamId());
    bb.putLong(request.getStreamOffset());
    bb.putLong(data.remaining());
    bb.flip();
    list.add(Unpooled.wrappedBuffer(bb, data));
  }
}
