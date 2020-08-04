package org.apache.ratis.datastream.client;

import org.apache.ratis.datastream.objects.DataStreamReply;
import org.apache.ratis.datastream.objects.DataStreamRequest;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * A client interface that sends request to the streaming pipeline.
 * Associated with it will be a Netty Client.
 */
public interface DataStreamClient {
  /**
   * send to server via streaming.
   * Return a completable future.
   */
  CompletableFuture<DataStreamReply> sendAsync(DataStreamRequest request);

  /**
   * receive a reply from the client and set the necessary future.
   * Invoked by the Netty Client associated with the object.
   */
  void setFuture(DataStreamReply reply);
}
