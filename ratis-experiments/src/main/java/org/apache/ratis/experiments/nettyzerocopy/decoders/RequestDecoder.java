package org.apache.ratis.experiments.nettyzerocopy.decoders;

import org.apache.ratis.experiments.nettyzerocopy.objects.RequestData;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.ratis.thirdparty.io.netty.handler.codec.ByteToMessageDecoder;
import java.util.List;

public class RequestDecoder extends ByteToMessageDecoder {
  @Override
  protected void decode(ChannelHandlerContext ctx,
                        ByteBuf msg, List<Object> out) throws Exception {
    if(msg.readableBytes() >= 8){
      int id = msg.readInt();
      int buflen = msg.readInt();
      if(msg.readableBytes() >= buflen){
        RequestData req = new RequestData();
        req.setDataId(id);
        //System.out.printf("msg id and buflen %d and %d bytes\n", id, buflen, msg.readableBytes());
        try {
          ByteBuf bf = msg.slice(msg.readerIndex(), buflen);
          req.setBuff(bf.nioBuffer());
        } catch (Exception e) {
          System.out.println(e);
        }
        msg.readerIndex(msg.readerIndex() + buflen);
        msg.markReaderIndex();
        out.add(req);
      } else{
        msg.resetReaderIndex();
        return;
      }
    } else{
      return;
    }
  }
}