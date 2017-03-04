/**
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

import org.apache.ratis.client.impl.ClientImplUtils;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Objects;

/** A client who sends requests to a raft service. */
public interface RaftClient extends Closeable {
  Logger LOG = LoggerFactory.getLogger(RaftClient.class);

  /** @return the id of this client. */
  ClientId getId();

  /** @return the client rpct. */
  RaftClientRpc getClientRpc();

  /**
   * Send the given message to the raft service.
   * The message may change the state of the service.
   * For readonly messages, use {@link #sendReadOnly(Message)} instead.
   */
  RaftClientReply send(Message message) throws IOException;

  /** Send the given readonly message to the raft service. */
  RaftClientReply sendReadOnly(Message message) throws IOException;

  /** Send set configuration request to the raft service. */
  RaftClientReply setConfiguration(RaftPeer[] serversInNewConf) throws IOException;

  /** @return a {@link Builder}. */
  static Builder newBuilder() {
    return new Builder();
  }

  /** To build {@link RaftClient} objects. */
  class Builder {
    private ClientId clientId;
    private RaftClientRpc clientRpc;
    private Collection<RaftPeer> servers;
    private RaftPeerId leaderId;
    private RaftProperties properties;
    private int retryInterval = RaftClientConfigKeys.RAFT_RPC_TIMEOUT_MS_DEFAULT;

    private Builder() {}

    /** @return a {@link RaftClient} object. */
    public RaftClient build() {
      if (clientId == null) {
        clientId = ClientId.createId();
      }
      if (properties != null) {
        retryInterval = properties.getInt(
            RaftClientConfigKeys.RAFT_RPC_TIMEOUT_MS_KEY,
            RaftClientConfigKeys.RAFT_RPC_TIMEOUT_MS_DEFAULT);
      }
      return ClientImplUtils.newRaftClient(clientId,
          Objects.requireNonNull(servers, "The 'servers' field is not initialized."),
          leaderId,
          Objects.requireNonNull(clientRpc, "The 'clientRpc' field is not initialized."),
          retryInterval);
    }

    /** Set {@link RaftClient} ID. */
    public Builder setClientId(ClientId clientId) {
      this.clientId = clientId;
      return this;
    }

    /** Set servers. */
    public Builder setServers(Collection<RaftPeer> servers) {
      this.servers = servers;
      return this;
    }

    /** Set leader ID. */
    public Builder setLeaderId(RaftPeerId leaderId) {
      this.leaderId = leaderId;
      return this;
    }

    /** Set {@link RaftClientRpc}. */
    public Builder setClientRpc(RaftClientRpc clientRpc) {
      this.clientRpc = clientRpc;
      return this;
    }

    /** Set {@link RaftProperties}. */
    public Builder setProperties(RaftProperties properties) {
      this.properties = properties;
      return this;
    }
  }
}
