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
package org.apache.ratis.hadooprpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.ratis.MiniRaftCluster;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.RpcType;
import org.apache.ratis.client.RaftClientRequestSender;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.hadooprpc.client.HadoopClientRequestSender;
import org.apache.ratis.hadooprpc.server.HadoopRpcServerConfigKeys;
import org.apache.ratis.hadooprpc.server.HadoopRpcService;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.impl.DelayLocalExecutionInjection;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MiniRaftClusterWithHadoopRpc extends MiniRaftCluster.RpcBase {
  static final Logger LOG = LoggerFactory.getLogger(MiniRaftClusterWithHadoopRpc.class);

  public static class Factory extends MiniRaftCluster.Factory<MiniRaftClusterWithHadoopRpc> {
    @Override
    public MiniRaftClusterWithHadoopRpc newCluster(
        String[] ids, RaftProperties prop, boolean formatted) {
      final Configuration conf = new Configuration();
      return newCluster(ids, prop, conf, formatted);
    }

    public MiniRaftClusterWithHadoopRpc newCluster(
        int numServers, RaftProperties properties, Configuration conf) {
      return newCluster(generateIds(numServers, 0), properties, conf, true);
    }

    public MiniRaftClusterWithHadoopRpc newCluster(
        String[] ids, RaftProperties prop, Configuration conf, boolean formatted) {
      RaftConfigKeys.Rpc.setType(prop::setEnum, RpcType.HADOOP);
      HadoopRpcServerConfigKeys.Ipc.setAddress(conf::set, "0.0.0.0:0");
      return new MiniRaftClusterWithHadoopRpc(ids, prop, conf, formatted);
    }
  }

  public static final Factory FACTORY = new Factory();

  public static final DelayLocalExecutionInjection sendServerRequest =
      new DelayLocalExecutionInjection(HadoopRpcService.SEND_SERVER_REQUEST);

  private final Configuration hadoopConf;

  private MiniRaftClusterWithHadoopRpc(String[] ids, RaftProperties properties,
      Configuration hadoopConf, boolean formatted) {
    super(ids, properties, formatted);
    this.hadoopConf = hadoopConf;
    getServers().stream().forEach(s -> setConf(s));
  }

  private void setConf(RaftServerImpl server) {
    final Configuration conf = new Configuration(hadoopConf);
    final String address = "0.0.0.0:" + getPort(server);
    HadoopRpcServerConfigKeys.Ipc.setAddress(conf::set, address);
    ((HadoopFactory)server.getFactory()).setConf(conf);
  }

  @Override
  protected RaftServerImpl newRaftServer(RaftPeerId id, boolean format) {
    final RaftServerImpl s = super.newRaftServer(id, format);
    if (hadoopConf != null) {
      setConf(s);
    }
    return s;
  }

  @Override
  public RaftClientRequestSender getRaftClientRequestSender() {
    return new HadoopClientRequestSender(getPeers(), hadoopConf);
  }

  @Override
  public void blockQueueAndSetDelay(String leaderId, int delayMs)
      throws InterruptedException {
    RaftTestUtil.blockQueueAndSetDelay(getServers(), sendServerRequest,
        leaderId, delayMs, getMaxTimeout());
  }
}
