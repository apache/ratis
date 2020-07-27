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

package org.apache.ratis.experiments.nettyzerocopy.encoders;

import org.apache.ratis.experiments.nettyzerocopy.objects.ResponseData;
import org.apache.ratis.thirdparty.io.netty.buffer.Unpooled;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.ratis.thirdparty.io.netty.handler.codec.MessageToMessageEncoder;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Encoder class for {@link ResponseData}
 * Writes ID of the response to the outbound message.
 */

public class ResponseEncoder
    extends MessageToMessageEncoder<ResponseData> {

  @Override
  protected void encode(ChannelHandlerContext channelHandlerContext,
                        ResponseData responseData, List<Object> list) throws Exception {

    ByteBuffer bb = ByteBuffer.allocateDirect(4);
    bb.putInt(responseData.getId());
    bb.flip();
    list.add(Unpooled.wrappedBuffer(bb));
  }
}