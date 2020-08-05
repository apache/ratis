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
package org.apache.ratis.client;

import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.datastream.DataStreamApi;
import org.apache.ratis.client.impl.DataStreamClientImpl;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.datastream.SupportedDataStreamType;
import org.apache.ratis.datastream.objects.DataStreamReply;
import org.apache.ratis.datastream.objects.DataStreamRequest;
import org.apache.ratis.protocol.ClientId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * A client interface that sends request to the streaming pipeline.
 * Associated with it will be a Netty Client.
 */
public interface DataStreamClient {

  Logger LOG = LoggerFactory.getLogger(DataStreamClient.class);

  /** Return Client id. */
  ClientId getId();

  /** Return Streamer Api instance. */
  DataStreamApi getDataStreamApi();

  /** Return Network transfer Api instance. */
  RaftClientStream getNetworkTransferApi();

  /**
   * send to server via streaming.
   * Return a completable future.
   */
  CompletableFuture<DataStreamReply> sendAsync(DataStreamRequest request);

  /** To build {@link DataStreamClient} objects */
  class Builder {
    private ClientId clientId;
    private RaftClientStream networkTransferApi;
    private RaftProperties properties;
    private Parameters parameters;

    private Builder() {}

    public DataStreamClientImpl build(){
      if (clientId == null) {
        clientId = ClientId.randomId();
      }
      if (properties != null) {
        if (networkTransferApi == null) {
          final SupportedDataStreamType streamType = RaftConfigKeys.DataStream.type(properties, LOG::debug);
          networkTransferApi = DataStreamClientFactory.cast(streamType.newFactory(parameters))
                                         .newRaftClientStream(clientId, properties);
        }
      }
      return new DataStreamClientImpl(clientId, properties, networkTransferApi);
    }

    public Builder setClientId(ClientId clientId) {
      this.clientId = clientId;
      return this;
    }

    public Builder setParameters(Parameters parameters) {
      this.parameters = parameters;
      return this;
    }

    public Builder setNetworkTransferApi(RaftClientStream networkTransferApi){
      this.networkTransferApi = networkTransferApi;
      return this;
    }

    public Builder setProperties(RaftProperties properties) {
      this.properties = properties;
      return this;
    }
  }

}
