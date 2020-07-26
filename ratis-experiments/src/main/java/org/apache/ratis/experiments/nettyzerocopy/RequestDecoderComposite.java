package org.apache.ratis.experiments.nettyzerocopy;

import org.apache.ratis.thirdparty.io.netty.handler.codec.ByteToMessageDecoder;

public class RequestDecoderComposite extends RequestDecoder {

  public RequestDecoderComposite() {
    this.setCumulator(ByteToMessageDecoder.COMPOSITE_CUMULATOR);
  }
}