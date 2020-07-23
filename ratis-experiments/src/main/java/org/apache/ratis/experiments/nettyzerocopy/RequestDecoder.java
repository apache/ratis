package org.apache.ratis.experiments.nettyzerocopy;

import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.ratis.thirdparty.io.netty.handler.codec.MessageToMessageDecoder;
import org.apache.ratis.thirdparty.io.netty.handler.codec.ReplayingDecoder;

import java.nio.ByteBuffer;
import java.util.List;

public class RequestDecoder extends ReplayingDecoder<RequestData> {
  @Override
  protected void decode(ChannelHandlerContext ctx,
                        ByteBuf msg, List<Object> out) throws Exception {
    RequestData req = new RequestData();
    req.setDataId(msg.readInt());
    System.out.println(req.getDataId());
//    int len = msg.readInt();
//    req.setBuff(ByteBuffer.wrap(msg.slice(msg.readerIndex(), len).array()));
    out.add(req);
  }
}