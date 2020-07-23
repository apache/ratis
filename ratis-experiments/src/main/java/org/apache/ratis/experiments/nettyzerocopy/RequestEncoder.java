package org.apache.ratis.experiments.nettyzerocopy;

import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.ratis.thirdparty.io.netty.buffer.CompositeByteBuf;
import org.apache.ratis.thirdparty.io.netty.buffer.Unpooled;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.ratis.thirdparty.io.netty.handler.codec.MessageToByteEncoder;

import java.nio.ByteBuffer;

public class RequestEncoder
    extends MessageToByteEncoder<RequestData> {

  @Override
  protected void encode(ChannelHandlerContext channelHandlerContext,
                        RequestData requestData, ByteBuf byteBuf) throws Exception {
    ByteBuffer bb = ByteBuffer.allocate(4);
    bb.putInt(requestData.getDataId());
    //bb.putInt(requestData.getBuff().capacity());
    //ByteBuf bf = Unpooled.wrappedBuffer(bb);//, requestData.getBuff());
    System.out.println(bb.capacity());
    byteBuf.writeInt(requestData.getDataId());
  }
}