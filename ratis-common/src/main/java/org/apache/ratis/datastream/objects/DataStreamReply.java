package org.apache.ratis.datastream.objects;

import java.nio.ByteBuffer;

public interface DataStreamReply {
  long getStreamId();
  long getMessageOffset();
  ByteBuffer getResponse();
}
