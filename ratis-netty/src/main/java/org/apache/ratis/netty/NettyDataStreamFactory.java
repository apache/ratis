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
package org.apache.ratis.netty;

import org.apache.ratis.client.DataStreamClientRpc;
import org.apache.ratis.client.DataStreamClientFactory;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.datastream.SupportedDataStreamType;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.DataStreamServerRpc;
import org.apache.ratis.server.DataStreamServerFactory;

public class NettyDataStreamFactory implements DataStreamServerFactory, DataStreamClientFactory {
  public NettyDataStreamFactory(Parameters parameters){}

  @Override
  public SupportedDataStreamType getDataStreamType() {
    return SupportedDataStreamType.NETTY;
  }

  @Override
  public DataStreamClientRpc newDataStreamClientRpc(ClientId clientId, RaftProperties properties) {
    return null;
  }

  @Override
  public DataStreamServerRpc newDataStreamServerRpc(RaftServer server) {
    return null;
  }
}
