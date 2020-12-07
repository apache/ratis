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
package org.apache.ratis.hadooprpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.ratis.client.ClientFactory;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.hadooprpc.client.HadoopClientRpc;
import org.apache.ratis.hadooprpc.server.HadoopRpcService;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.ServerFactory;

public class HadoopFactory implements ServerFactory, ClientFactory {
  public static Parameters newRaftParameters(Configuration conf) {
    final Parameters p = new Parameters();
    HadoopConfigKeys.setConf(p, conf);
    return p;
  }

  private final Configuration conf;

  public HadoopFactory(Parameters parameters) {
    this(HadoopConfigKeys.getConf(parameters));
  }

  public HadoopFactory(Configuration conf) {
    this.conf = conf != null? conf: new Configuration();
  }

  public Configuration getConf() {
    return conf;
  }

  @Override
  public SupportedRpcType getRpcType() {
    return SupportedRpcType.HADOOP;
  }

  @Override
  public HadoopRpcService newRaftServerRpc(RaftServer server) {
    return HadoopRpcService.newBuilder()
        .setServer(server)
        .setConf(getConf())
        .build();
  }

  @Override
  public HadoopClientRpc newRaftClientRpc(ClientId clientId, RaftProperties properties) {
    return new HadoopClientRpc(clientId, getConf());
  }
}
