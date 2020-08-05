package org.apache.ratis.client;

import org.apache.ratis.datastream.objects.DataStreamReply;
import org.apache.ratis.datastream.objects.DataStreamRequest;
import org.apache.ratis.util.JavaUtils;

import java.util.concurrent.CompletableFuture;

/**
 * An api interface for to stream from client to server.
 */
public interface ClientStreamApi {

  /** Async call to send a request. */
  default CompletableFuture<DataStreamReply> sendRequestAsync(DataStreamRequest request) {
    throw new UnsupportedOperationException(getClass() + " does not support "
        + JavaUtils.getCurrentStackTraceElement().getMethodName());
  }

}
