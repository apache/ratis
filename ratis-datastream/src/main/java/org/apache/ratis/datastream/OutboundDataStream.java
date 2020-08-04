package org.apache.ratis.datastream;

import org.apache.ratis.datastream.objects.DataStreamReply;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public interface OutboundDataStream extends AutoCloseable {
  CompletableFuture<RaftClientReply> sendAsync(ByteBuf buf, long streamId, long packetId);

  CompletableFuture<DataStreamReply> closeAsync();

  default void close() throws Exception {
    try {
      closeAsync().get();
    } catch (ExecutionException e) {
      final Throwable cause = e.getCause();
      throw cause instanceof Exception? (Exception)cause: e;
    }
  }
}
