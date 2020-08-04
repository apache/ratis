package org.apache.ratis.datastream;

import org.apache.ratis.datastream.objects.DataStreamReply;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.ratis.util.SizeInBytes;

import java.util.concurrent.CompletableFuture;


/**
 * An interface for streaming data.
 * Associated with it's implementation will be a client.
 */

public interface DataStreamApi {

  /**
   * Create a new stream for a new streamToRatis invocation
   * allows multiple stream from a single client.
   */
  OutboundDataStream newStream();

  /**
   * stream large files to raft group from client.
   * Returns a future of the final stream packet to indicate completion of stream.
   * Bytebuffer needs to be direct for zero-copy semantics.
   *
   */
  CompletableFuture<DataStreamReply> streamToRatis(ByteBuf message, SizeInBytes packetSize);

}
