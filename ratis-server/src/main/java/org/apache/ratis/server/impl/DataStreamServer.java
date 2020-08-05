package org.apache.ratis.server.impl;

import org.apache.ratis.datastream.objects.DataStreamReply;
import org.apache.ratis.datastream.objects.DataStreamRequest;

import java.util.concurrent.CompletableFuture;

/**
 * A server interface handling incoming streams
 * Relays those streams to other servers after persisting
 * It will have an associated Netty client, server for streaming and listening.
 */
public interface DataStreamServer {
  /**
   * Invoked from the server to persist data and add to relay queue.
   */
  boolean handleRequest(DataStreamRequest request);

  /**
   * Poll the queue and trigger streaming for messages in relay queue.
   */
  CompletableFuture<DataStreamReply> streamAsync(DataStreamRequest request);

  /**
   * receive a reply from the client and set the necessary future.
   * Invoked by the Netty Client associated with the object.
   */
  CompletableFuture<DataStreamReply> setReply(DataStreamReply reply);
}
