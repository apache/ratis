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

import org.apache.ratis.client.DataStreamClient;
import org.apache.ratis.client.DataStreamClientRpc;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientRpc;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftPeerId;

/** Client utilities for internal use. */
public interface ClientImplUtils {
  static RaftClient newRaftClient(ClientId clientId, RaftGroup group,
      RaftPeerId leaderId, RaftPeer primaryDataStreamServer, RaftClientRpc clientRpc, RaftProperties properties,
      RetryPolicy retryPolicy) {
    return new RaftClientImpl(clientId, group, leaderId, primaryDataStreamServer, clientRpc, properties,
        retryPolicy);
  }

  static DataStreamClient newDataStreamClient(ClientId clientId, RaftGroupId groupId, RaftPeer primaryDataStreamServer,
      DataStreamClientRpc dataStreamClientRpc, RaftProperties properties) {
    return new DataStreamClientImpl(clientId, groupId, primaryDataStreamServer, dataStreamClientRpc, properties);
  }
}
