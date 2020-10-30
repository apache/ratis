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

import org.apache.ratis.client.api.DataStreamApi;
import org.apache.ratis.client.impl.DataStreamClientImpl;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftPeer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;

/**
 * A user interface extending {@link DataStreamApi}.
 */
public interface DataStreamClient extends DataStreamApi, Closeable {
  Logger LOG = LoggerFactory.getLogger(DataStreamClient.class);

  /** Return the rpc client instance **/
  DataStreamClientRpc getClientRpc();

  static Builder newBuilder() {
    return new Builder();
  }

  /** To build {@link DataStreamClient} objects */
  class Builder {
    private RaftPeer raftServer;
    private RaftProperties properties;
    private Parameters parameters;

    private Builder() {}

    public DataStreamClientImpl build(){
      return new DataStreamClientImpl(raftServer, properties, parameters);
    }

    public Builder setRaftServer(RaftPeer peer) {
      this.raftServer = peer;
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
