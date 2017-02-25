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
package org.apache.ratis.server.simulation;

import org.apache.ratis.server.RaftServerRpc;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.impl.ServerFactory;

import java.util.Objects;

public class SimulationFactory extends ServerFactory.BaseFactory {
  private SimulatedRequestReply<RaftServerRequest, RaftServerReply> serverRequestReply;
  private SimulatedClientRequestReply client2serverRequestReply;

  public void initRpc(
      SimulatedRequestReply<RaftServerRequest, RaftServerReply> serverRequestReply,
      SimulatedClientRequestReply client2serverRequestReply) {
    this.serverRequestReply = Objects.requireNonNull(serverRequestReply);
    this.client2serverRequestReply = Objects.requireNonNull(client2serverRequestReply);
  }

  @Override
  public RaftServerRpc newRaftServerRpc(RaftServerImpl server) {
    return new SimulatedServerRpc(server, serverRequestReply, client2serverRequestReply);
  }
}
