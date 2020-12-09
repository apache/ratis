/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.ratis.server.impl;

import java.io.IOException;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.datastream.SupportedDataStreamType;
import org.apache.ratis.server.DataStreamServer;
import org.apache.ratis.server.DataStreamServerFactory;
import org.apache.ratis.server.DataStreamServerRpc;
import org.apache.ratis.server.RaftServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DataStreamServerImpl implements DataStreamServer {
  public static final Logger LOG = LoggerFactory.getLogger(DataStreamServerImpl.class);

  private final DataStreamServerRpc serverRpc;

  DataStreamServerImpl(RaftServer server, Parameters parameters) {
    final SupportedDataStreamType type = RaftConfigKeys.DataStream.type(server.getProperties(), LOG::info);
    this.serverRpc = DataStreamServerFactory.newInstance(type, parameters).newDataStreamServerRpc(server);
  }

  @Override
  public DataStreamServerRpc getServerRpc() {
    return serverRpc;
  }

  @Override
  public void close() throws IOException {
    serverRpc.close();
  }
}
