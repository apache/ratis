package org.apache.ratis.netty.decoders;

import org.apache.ratis.protocol.DataStreamReply;
import org.apache.ratis.protocol.DataStreamReplyImpl;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.ratis.thirdparty.io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

public class DataStreamReplyDecoder extends ByteToMessageDecoder {
  @Override
  protected void decode(ChannelHandlerContext channelHandlerContext,
                        ByteBuf byteBuf, List<Object> list) throws Exception {
    if(byteBuf.readableBytes() >= 24){
      long streamId = byteBuf.readLong();
      long dataOffset = byteBuf.readLong();
      long dataLength = byteBuf.readLong();
      if(byteBuf.readableBytes() >= dataLength){
        DataStreamReplyImpl reply = new DataStreamReplyImpl(streamId,
            dataOffset,
            byteBuf.slice(byteBuf.readerIndex(), (int) dataLength).nioBuffer());
        byteBuf.readerIndex(byteBuf.readerIndex() + (int)dataLength);
        byteBuf.markReaderIndex();
        list.add(reply);
      } else {
        byteBuf.resetReaderIndex();
        return;
      }
    } else{
      return;
    }
  }
}
