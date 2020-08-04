package org.apache.ratis.datastream.objects;

public class DataStreamReply {
  long streamId;
  long messageId;
  String response;
}
