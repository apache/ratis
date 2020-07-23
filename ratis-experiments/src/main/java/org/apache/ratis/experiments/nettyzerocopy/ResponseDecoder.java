package org.apache.ratis.experiments.nettyzerocopy;


import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.ratis.thirdparty.io.netty.handler.codec.ReplayingDecoder;

import java.nio.ByteBuffer;
import java.util.List;

public class ResponseDecoder
    extends ReplayingDecoder<ResponseData> {

  @Override
  protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf, List<Object> list) throws Exception {
    ResponseData reply = new ResponseData();
    reply.setId(byteBuf.readInt());
    list.add(reply);
  }
}