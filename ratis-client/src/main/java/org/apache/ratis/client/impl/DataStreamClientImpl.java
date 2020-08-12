/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package org.apache.ratis.client.impl;

import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.DataStreamClient;
import org.apache.ratis.client.DataStreamClientFactory;
import org.apache.ratis.client.DataStreamClientRpc;
import org.apache.ratis.client.api.DataStreamOutput;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.datastream.SupportedDataStreamType;
import org.apache.ratis.protocol.DataStreamReply;
import org.apache.ratis.protocol.RaftPeer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Streaming client implementation
 * allows client to create streams and send asynchronously.
 */

public class DataStreamClientImpl implements DataStreamClient {
  public static final Logger LOG = LoggerFactory.getLogger(DataStreamClientImpl.class);

  private DataStreamClientRpc dataStreamClientRpc;
  private OrderedStreamAsync orderedStreamAsync;
  private RaftPeer raftServer;
  private RaftProperties properties;
  private Parameters parameters;
  private long streamId = 0;

  public DataStreamClientImpl(RaftPeer raftServer,
                              RaftProperties properties,
                              Parameters parameters) {
    this.raftServer = Objects.requireNonNull(raftServer,
                                          "peer == null");
    this.properties = properties;
    this.parameters = parameters;

    final SupportedDataStreamType type = RaftConfigKeys.DataStream.type(properties, LOG::info);
    this.dataStreamClientRpc = DataStreamClientFactory.cast(type.newFactory(parameters))
                               .newDataStreamClientRpc(raftServer, properties);

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
  public DataStreamClientRpc getClientRpc() {
    return dataStreamClientRpc;
  }

  @Override
  public DataStreamOutput stream() {
    streamId++;
    return new DataStreamOutputImpl(streamId);
  }

  @Override
  public void addPeers(Iterable<RaftPeer> peers) {
    return;
  }

  @Override
  public void close(){
    dataStreamClientRpc.closeClient();
  }

  public void start(){
    dataStreamClientRpc.startClient();
  }
}
