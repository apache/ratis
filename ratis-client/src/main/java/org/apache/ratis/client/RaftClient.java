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
import org.apache.ratis.client.api.AdminApi;
import org.apache.ratis.client.api.AsyncApi;
import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.client.api.DataStreamApi;
import org.apache.ratis.client.api.GroupManagementApi;
import org.apache.ratis.client.api.LeaderElectionManagementApi;
import org.apache.ratis.client.api.MessageStreamApi;
import org.apache.ratis.client.api.SnapshotManagementApi;
import org.apache.ratis.client.impl.ClientImplUtils;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.*;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.rpc.RpcType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Collection;
import java.util.Objects;

/** A client who sends requests to a raft service. */
public interface RaftClient extends Closeable {
  Logger LOG = LoggerFactory.getLogger(RaftClient.class);

  /** @return the id of this client. */
  ClientId getId();

  /** @return the cluster leaderId recorded by this client. */
  RaftPeerId getLeaderId();

  /** @return the {@link RaftClientRpc}. */
  RaftClientRpc getClientRpc();

  /** @return the {@link AdminApi}. */
  AdminApi admin();

  /** Get the {@link GroupManagementApi} for the given server. */
  GroupManagementApi getGroupManagementApi(RaftPeerId server);

  /** Get the {@link SnapshotManagementApi} for the given server. */
  SnapshotManagementApi getSnapshotManagementApi();

  /** Get the {@link SnapshotManagementApi} for the given server. */
  SnapshotManagementApi getSnapshotManagementApi(RaftPeerId server);

  /** Get the {@link LeaderElectionManagementApi} for the given server. */
  LeaderElectionManagementApi getLeaderElectionManagementApi(RaftPeerId server);

  /** @return the {@link BlockingApi}. */
  BlockingApi io();

  /** Get the {@link AsyncApi}. */
  AsyncApi async();

  /** @return the {@link MessageStreamApi}. */
  MessageStreamApi getMessageStreamApi();

  /** @return the {@link DataStreamApi}. */
  DataStreamApi getDataStreamApi();

  /** @return a {@link Builder}. */
  static Builder newBuilder() {
    return new Builder();
  }

  /** To build {@link RaftClient} objects. */
  class Builder {
    private ClientId clientId;
    private RaftClientRpc clientRpc;
    private RaftGroup group;
    private RaftPeerId leaderId;
    private RaftPeer primaryDataStreamServer;
    private RaftProperties properties;
    private Parameters parameters;
    private RetryPolicy retryPolicy = RetryPolicies.retryForeverNoSleep();

    private Builder() {}

    /** @return a {@link RaftClient} object. */
    public RaftClient build() {
      if (clientId == null) {
        clientId = ClientId.randomId();
      }
      if (properties != null) {
        if (clientRpc == null) {
          final RpcType rpcType = RaftConfigKeys.Rpc.type(properties, LOG::debug);
          final ClientFactory factory = ClientFactory.cast(rpcType.newFactory(parameters));
          clientRpc = factory.newRaftClientRpc(clientId, properties);
        }
      }
      Objects.requireNonNull(group, "The 'group' field is not initialized.");
      if (primaryDataStreamServer == null) {
        final Collection<RaftPeer> peers = group.getPeers();
        if (!peers.isEmpty()) {
          primaryDataStreamServer = peers.iterator().next();
        }
      }
      return ClientImplUtils.newRaftClient(clientId, group, leaderId, primaryDataStreamServer,
          Objects.requireNonNull(clientRpc, "The 'clientRpc' field is not initialized."),
          properties, retryPolicy);
    }

    /** Set {@link RaftClient} ID. */
    public Builder setClientId(ClientId clientId) {
      this.clientId = clientId;
      return this;
    }

    /** Set servers. */
    public Builder setRaftGroup(RaftGroup grp) {
      this.group = grp;
      return this;
    }

    /** Set leader ID. */
    public Builder setLeaderId(RaftPeerId leaderId) {
      this.leaderId = leaderId;
      return this;
    }

    /** Set primary server of DataStream. */
    public Builder setPrimaryDataStreamServer(RaftPeer primaryDataStreamServer) {
      this.primaryDataStreamServer = primaryDataStreamServer;
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

    /** Set {@link Parameters}. */
    public Builder setParameters(Parameters parameters) {
      this.parameters = parameters;
      return this;
    }

    /** Set {@link RetryPolicy}. */
    public Builder setRetryPolicy(RetryPolicy retryPolicy) {
      this.retryPolicy = retryPolicy;
      return this;
    }
  }
}
