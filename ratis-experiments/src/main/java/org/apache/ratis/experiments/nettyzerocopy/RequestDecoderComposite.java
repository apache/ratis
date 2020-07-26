package org.apache.ratis.experiments.nettyzerocopy;

import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.ratis.thirdparty.io.netty.handler.codec.ByteToMessageDecoder;
import java.util.List;

public class RequestDecoderComposite extends RequestDecoder {

  public RequestDecoderComposite() {
    this.setCumulator(ByteToMessageDecoder.COMPOSITE_CUMULATOR);
  }
}