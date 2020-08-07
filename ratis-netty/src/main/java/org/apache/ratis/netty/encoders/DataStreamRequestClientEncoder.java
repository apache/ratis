package org.apache.ratis.netty.encoders;

import org.apache.ratis.protocol.DataStreamRequestClient;
import org.apache.ratis.thirdparty.io.netty.buffer.Unpooled;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.ratis.thirdparty.io.netty.handler.codec.MessageToMessageEncoder;

import java.nio.ByteBuffer;
import java.util.List;

public class DataStreamRequestClientEncoder
    extends MessageToMessageEncoder<DataStreamRequestClient> {

  private ByteBuffer bb = ByteBuffer.allocateDirect(24);

  @Override
  protected void encode(ChannelHandlerContext channelHandlerContext,
                        DataStreamRequestClient requestData, List<Object> list) throws Exception {
    bb.position(0);
    bb.putLong(requestData.getStreamId());
    bb.putLong(requestData.getDataOffset());
    bb.putLong(requestData.getDataLength());
    bb.flip();
    list.add(Unpooled.wrappedBuffer(bb, requestData.getBuf()));
  }
}
