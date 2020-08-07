package org.apache.ratis.client.impl;

import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.DataStreamClientFactory;
import org.apache.ratis.client.DataStreamClientRpc;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.client.api.DataStreamApi;
import org.apache.ratis.client.api.DataStreamOutput;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.datastream.SupportedDataStreamType;
import org.apache.ratis.protocol.DataStreamReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Streaming client implementation
 * allows client to create streams and send asynchronously.
 */

public class DataStreamImpl implements DataStreamApi {
  public static final Logger LOG = LoggerFactory.getLogger(DataStreamImpl.class);

  private DataStreamClientRpc dataStreamClientRpc;
  private OrderedStreamAsync orderedStreamAsync;
  private RaftClientImpl client;
  private RaftProperties properties;
  private Parameters parameters;
  private long streamId = 0;

  public DataStreamImpl(RaftClientImpl client, RaftProperties properties, Parameters parameters) {
    this.client = Objects.requireNonNull(client, "client == null");
    this.properties = properties;
    this.parameters = parameters;

    final SupportedDataStreamType type = RaftConfigKeys.DataStream.type(properties, LOG::info);
    this.dataStreamClientRpc = DataStreamClientFactory.cast(type.newFactory(parameters))
                               .newDataStreamClientRpc(client, properties);

    this.orderedStreamAsync = new OrderedStreamAsync(dataStreamClientRpc, properties);
  }

  class DataStreamOutputImpl implements DataStreamOutput {
    private long streamId = 0;
    private long messageId = 0;

    public DataStreamOutputImpl(long id){
      this.streamId = id;
    }

    // send to the attached dataStreamClientRpc
    @Override
    public CompletableFuture<DataStreamReply> streamAsync(ByteBuffer buf) {
      messageId++;
      return orderedStreamAsync.sendRequest(streamId, messageId, buf);
    }

    // should wait for attached sliding window to terminate
    @Override
    public CompletableFuture<DataStreamReply> closeAsync() {
      return null;
    }
  }

  @Override
  public DataStreamOutput stream() {
    streamId++;
    return new DataStreamOutputImpl(streamId);
  }
}
