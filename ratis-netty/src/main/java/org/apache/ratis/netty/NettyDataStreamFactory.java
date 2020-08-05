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

import org.apache.ratis.client.ClientStreamApi;
import org.apache.ratis.client.StreamClientFactory;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.datastream.SupportedStreamType;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.impl.DataStreamServer;
import org.apache.ratis.server.impl.ServerStreamFactory;

public class NettyDataStreamFactory implements ServerStreamFactory, StreamClientFactory {
  public NettyDataStreamFactory(Parameters parameters){}

  @Override
  public SupportedStreamType getStreamType() {
    return SupportedStreamType.NETTY;
  }

  @Override
  public ClientStreamApi newClientStreamApi(ClientId clientId, RaftProperties properties) {
    return null;
  }

  @Override
  public DataStreamServer newServerStreamApi(RaftServer server) {
    return null;
  }
}
