package org.apache.ratis.experiments.nettyzerocopy;

import org.apache.ratis.thirdparty.io.netty.buffer.Unpooled;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.ratis.thirdparty.io.netty.handler.codec.MessageToMessageEncoder;

import java.nio.ByteBuffer;
import java.util.List;

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