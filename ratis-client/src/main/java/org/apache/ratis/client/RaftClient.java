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

import com.google.common.base.Preconditions;
import org.apache.ratis.client.impl.ClientImplUtils;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

/** A client who sends requests to a raft service. */
public interface RaftClient extends Closeable {
  Logger LOG = LoggerFactory.getLogger(RaftClient.class);
  long DEFAULT_SEQNUM = 0;

  /** @return the id of this client. */
  String getId();

  /** @return the request sender of this client. */
  RaftClientRequestSender getRequestSender();

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
    private static final AtomicInteger COUNT = new AtomicInteger();

    private String clientId = RaftClient.class.getSimpleName() + COUNT.incrementAndGet();
    private RaftClientRequestSender requestSender;
    private Collection<RaftPeer> servers;
    private String leaderId;
    private RaftProperties properties;
    private int retryInterval = RaftClientConfigKeys.RAFT_RPC_TIMEOUT_MS_DEFAULT;

    private Builder() {}

    /** @return a {@link RaftClient} object. */
    public RaftClient build() {
      Preconditions.checkNotNull(requestSender);
      Preconditions.checkNotNull(servers);

      if (leaderId == null) {
        leaderId = servers.iterator().next().getId(); //use the first peer
      }
      if (properties != null) {
        retryInterval = properties.getInt(
            RaftClientConfigKeys.RAFT_RPC_TIMEOUT_MS_KEY,
            RaftClientConfigKeys.RAFT_RPC_TIMEOUT_MS_DEFAULT);
      }
      return ClientImplUtils.newRaftClient(clientId, servers, leaderId,
          requestSender, retryInterval);
    }

    /** Set {@link RaftClient} ID. */
    public Builder setClientId(String clientId) {
      this.clientId = clientId;
      return this;
    }

    /** Set servers. */
    public Builder setServers(Collection<RaftPeer> servers) {
      this.servers = servers;
      return this;
    }

    /** Set leader ID. */
    public Builder setLeaderId(String leaderId) {
      this.leaderId = leaderId;
      return this;
    }

    /** Set {@link RaftClientRequestSender}. */
    public Builder setRequestSender(RaftClientRequestSender requestSender) {
      this.requestSender = requestSender;
      return this;
    }

    /** Set {@link RaftProperties}. */
    public Builder setProperties(RaftProperties properties) {
      this.properties = properties;
      return this;
    }
  }
}
