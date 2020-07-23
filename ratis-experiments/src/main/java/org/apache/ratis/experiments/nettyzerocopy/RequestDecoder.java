package org.apache.ratis.experiments.nettyzerocopy;

public class RequestDecoder extends MessageToMessageDecoder<ByteBuf>{
  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {

  }
}