package org.apache.ratis.datastream.objects;

import org.apache.ratis.thirdparty.io.netty.buffer.CompositeByteBuf;

public interface DataStreamRequest {
  long getStreamId();
  long getMessageOffset();
  long getDataLength();
}
