package org.apache.ratis.protocol;

import java.nio.ByteBuffer;

public class DataStreamReplyImpl implements DataStreamReply {
  private long streamId;
  private long dataOffset;
  private ByteBuffer response;

  public DataStreamReplyImpl(long streamId,
                             long dataOffset,
                             ByteBuffer bf){
    this.streamId = streamId;
    this.dataOffset = dataOffset;
    this.response = bf;
  }

  @Override
  public ByteBuffer getResponse() {
    return response;
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
    return response.capacity();
  }
}
