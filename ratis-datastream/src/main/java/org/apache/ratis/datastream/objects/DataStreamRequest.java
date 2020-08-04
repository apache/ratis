package org.apache.ratis.datastream.objects;

import org.apache.ratis.thirdparty.io.netty.buffer.CompositeByteBuf;

public class DataStreamRequest {
  long streamId;
  long messageId;
  long messageLength; // in bytes
  CompositeByteBuf byteBuf;
}
