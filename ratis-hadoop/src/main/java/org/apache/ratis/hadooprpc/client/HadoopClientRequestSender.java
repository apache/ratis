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
package org.apache.ratis.hadooprpc.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.ratis.client.RaftClientRequestSender;
import org.apache.ratis.protocol.*;
import org.apache.ratis.util.PeerProxyMap;

import java.io.IOException;
import java.util.Collection;

public class HadoopClientRequestSender implements RaftClientRequestSender {

  private final PeerProxyMap<RaftClientProtocolClientSideTranslatorPB> proxies;

  public HadoopClientRequestSender(
      Collection<RaftPeer> peers, final Configuration conf) {
    this.proxies  = new PeerProxyMap<>(
        p -> new RaftClientProtocolClientSideTranslatorPB(p.getAddress(), conf));
    proxies.addPeers(peers);
  }

  @Override
  public RaftClientReply sendRequest(RaftClientRequest request)
      throws IOException {
    final RaftPeerId serverId = request.getServerId();
    final RaftClientProtocolClientSideTranslatorPB proxy =
        proxies.getProxy(serverId);
    try {
      if (request instanceof SetConfigurationRequest) {
        return proxy.setConfiguration((SetConfigurationRequest) request);
      } else {
        return proxy.submitClientRequest(request);
      }
    } catch (RemoteException e) {
      throw e.unwrapRemoteException(
          StateMachineException.class,
          ReconfigurationTimeoutException.class,
          ReconfigurationInProgressException.class,
          RaftException.class,
          LeaderNotReadyException.class);
    }
  }

  @Override
  public void addServers(Iterable<RaftPeer> servers) {
    proxies.addPeers(servers);
  }

  @Override
  public void close() {
    proxies.close();
  }
}
