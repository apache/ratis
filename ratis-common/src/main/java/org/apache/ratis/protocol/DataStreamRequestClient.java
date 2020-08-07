package org.apache.ratis.protocol;

import java.nio.ByteBuffer;

public class DataStreamRequestClient implements DataStreamRequest{
  private long streamId;
  private long dataOffset;
  private ByteBuffer buf;
  
  public DataStreamRequestClient(long streamId, long dataOffset,
                          ByteBuffer buf){
    this.streamId = streamId;
    this.dataOffset = dataOffset;
    this.buf = buf;
  }

  @Override
  public long getStreamId() {
    return streamId;
  }

  @Override
  public long getDataOffset() {
    return dataOffset;
  }

  @Override
  public long getDataLength() {
    return buf.capacity();
  }

  public ByteBuffer getBuf() {
    return buf;
  }
}
