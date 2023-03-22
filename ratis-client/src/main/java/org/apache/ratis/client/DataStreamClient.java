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
import org.apache.ratis.client.impl.ClientImplUtils;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.datastream.SupportedDataStreamType;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Objects;
import java.util.Optional;

/**
 * A user interface extending {@link DataStreamRpcApi}.
 */
public interface DataStreamClient extends DataStreamRpcApi, Closeable {
  Logger LOG = LoggerFactory.getLogger(DataStreamClient.class);

  /** Return the rpc client instance **/
  DataStreamClientRpc getClientRpc();

  static Builder newBuilder() {
    return new Builder();
  }

  /** To build {@link DataStreamClient} objects */
  final class Builder {
    private RaftPeer dataStreamServer;
    private DataStreamClientRpc dataStreamClientRpc;
    private RaftProperties properties;
    private Parameters parameters;
    private RaftGroupId raftGroupId;
    private ClientId clientId;

    private Builder() {}

    public DataStreamClient build() {
      Objects.requireNonNull(dataStreamServer, "The 'dataStreamServer' field is not initialized.");
      if (properties != null) {
        if (dataStreamClientRpc == null) {
          final SupportedDataStreamType type = RaftConfigKeys.DataStream.type(properties, LOG::info);
          dataStreamClientRpc = DataStreamClientFactory.newInstance(type, parameters)
              .newDataStreamClientRpc(dataStreamServer, properties);
        }
      }
      return ClientImplUtils.newDataStreamClient(
          Optional.ofNullable(clientId).orElseGet(ClientId::randomId),
          raftGroupId, dataStreamServer, dataStreamClientRpc, properties);
    }

    public Builder setClientId(ClientId clientId) {
      this.clientId = clientId;
      return this;
    }

    public Builder setRaftGroupId(RaftGroupId raftGroupId) {
      this.raftGroupId = raftGroupId;
      return this;
    }

    public Builder setDataStreamServer(RaftPeer dataStreamServer) {
      this.dataStreamServer = dataStreamServer;
      return this;
    }

    public Builder setDataStreamClientRpc(DataStreamClientRpc dataStreamClientRpc) {
      this.dataStreamClientRpc = dataStreamClientRpc;
      return this;
    }

    public Builder setParameters(Parameters parameters) {
      this.parameters = parameters;
      return this;
    }

    public Builder setProperties(RaftProperties properties) {
      this.properties = properties;
      return this;
    }
  }
}
