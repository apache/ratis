/**
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

package org.apache.ratis.experiments.nettyzerocopy.decoders;

import org.apache.ratis.experiments.nettyzerocopy.objects.RequestDataComposite;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.ratis.thirdparty.io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

/**
 * A decoder extending generic {@link ByteToMessageDecoder}
 * Reads from inbound message and checks if the message is valid.
 * If yes creates a {@link RequestDecoderComposite} instance and writes to inbound list.
 * zero-copy semantics. Utilizes COMPOSITE_CUMULATOR which creates a composite buffer of inbound messages.
 */

public class RequestDecoderComposite extends RequestDecoder {

  public RequestDecoderComposite() {
    this.setCumulator(ByteToMessageDecoder.COMPOSITE_CUMULATOR);
  }

  @Override
  protected void decode(ChannelHandlerContext ctx,
                        ByteBuf msg, List<Object> out) throws Exception {
    if(msg.readableBytes() >= 8){
      int id = msg.readInt();
      int buflen = msg.readInt();
      if(msg.readableBytes() >= buflen){
        RequestDataComposite req = new RequestDataComposite();
        req.setDataId(id);
        //System.out.printf("msg id and buflen %d and %d bytes\n", id, buflen, msg.readableBytes());
        try {
          ByteBuf bf = msg.slice(msg.readerIndex(), buflen);
          bf.retain();
          req.setBuff(bf);
        } catch (Exception e) {
          System.out.println(e);
        }
        msg.readerIndex(msg.readerIndex() + buflen);
        msg.markReaderIndex();
        out.add(req);
      } else{
        msg.resetReaderIndex();
        return;
      }
    } else{
      return;
    }
  }
}