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

import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.ratis.thirdparty.io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

public class DataStreamRequestDecoder extends ByteToMessageDecoder {

  public DataStreamRequestDecoder() {
    this.setCumulator(ByteToMessageDecoder.COMPOSITE_CUMULATOR);
  }

  @Override
  protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf, List<Object> list) {
    if(byteBuf.readableBytes() >= 24){
      long streamId = byteBuf.readLong();
      long dataOffset = byteBuf.readLong();
      long dataLength = byteBuf.readLong();
      if(byteBuf.readableBytes() >= dataLength){
        ByteBuf bf = byteBuf.slice(byteBuf.readerIndex(), (int)dataLength);
        bf.retain();
        final DataStreamRequestByteBuf req = new DataStreamRequestByteBuf(streamId, dataOffset, bf);
        byteBuf.readerIndex(byteBuf.readerIndex() + (int)dataLength);
        byteBuf.markReaderIndex();
        list.add(req);
      } else {
        byteBuf.resetReaderIndex();
      }
    }
  }
}
