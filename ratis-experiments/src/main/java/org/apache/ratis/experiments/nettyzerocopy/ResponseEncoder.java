package org.apache.ratis.experiments.nettyzerocopy;

import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.ratis.thirdparty.io.netty.buffer.Unpooled;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.ratis.thirdparty.io.netty.handler.codec.MessageToByteEncoder;

import java.nio.ByteBuffer;

public class ResponseEncoder
    extends MessageToByteEncoder<ResponseData> {

  @Override
  protected void encode(ChannelHandlerContext channelHandlerContext,
                        ResponseData requestData, ByteBuf byteBuf) throws Exception {
    ByteBuffer bb = ByteBuffer.allocate(4);
    bb.putInt(requestData.getId());
    ByteBuf bf = Unpooled.wrappedBuffer(bb);
    byteBuf.writeBytes(bf);
  }
}