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
package org.apache.ratis.hadooprpc.server;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngineShaded;
import org.apache.hadoop.ipc.RPC;
import org.apache.ratis.hadooprpc.Proxy;
import org.apache.ratis.hadooprpc.client.RaftClientProtocolPB;
import org.apache.ratis.hadooprpc.client.RaftClientProtocolServerSideTranslatorPB;
import org.apache.ratis.protocol.RaftClientProtocol;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.RaftServerRpc;
import org.apache.ratis.server.protocol.RaftServerProtocol;
import org.apache.ratis.shaded.com.google.protobuf.BlockingService;
import org.apache.ratis.shaded.com.google.protobuf.ServiceException;
import org.apache.ratis.shaded.proto.RaftProtos.*;
import org.apache.ratis.shaded.proto.hadoop.HadoopProtos.RaftClientProtocolService;
import org.apache.ratis.shaded.proto.hadoop.HadoopProtos.RaftServerProtocolService;
import org.apache.ratis.util.CodeInjectionForTesting;
import org.apache.ratis.util.PeerProxyMap;
import org.apache.ratis.util.ProtoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

/** Server side Hadoop RPC service. */
public class HadoopRpcService implements RaftServerRpc {
  public static final Logger LOG = LoggerFactory.getLogger(HadoopRpcService.class);
  static final String CLASS_NAME = HadoopRpcService.class.getSimpleName();
  public static final String SEND_SERVER_REQUEST = CLASS_NAME + ".sendServerRequest";

  public static class Builder extends RaftServerRpc.Builder<Builder, HadoopRpcService> {
    private Configuration conf;

    private Builder() {
      super(0);
    }

    public Configuration getConf() {
      return conf;
    }

    public Builder setConf(Configuration conf) {
      this.conf = conf;
      return this;
    }

    @Override
    public Builder getThis() {
      return this;
    }

    @Override
    public HadoopRpcService build() throws IOException {
      return new HadoopRpcService(getServer(), getConf());
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  private final String id;
  private final RPC.Server ipcServer;
  private final InetSocketAddress ipcServerAddress;

  private final PeerProxyMap<Proxy<RaftServerProtocolPB>> proxies;

  private HadoopRpcService(RaftServer server, final Configuration conf)
      throws IOException {
    this.proxies = new PeerProxyMap<>(
        p -> new Proxy(RaftServerProtocolPB.class, p.getAddress(), conf));
    this.id = server.getId();
    this.ipcServer = newRpcServer(server, conf);
    this.ipcServerAddress = ipcServer.getListenerAddress();

    addRaftClientProtocol(server, conf);

    LOG.info(getClass().getSimpleName() + " created RPC.Server at "
        + ipcServerAddress);
  }

  @Override
  public InetSocketAddress getInetSocketAddress() {
    return ipcServerAddress;
  }

  private RPC.Server newRpcServer(RaftServerProtocol serverProtocol, final Configuration conf)
      throws IOException {
    final RaftServerConfigKeys.Get get = new RaftServerConfigKeys.Get() {
      @Override
      protected int getInt(String key, int defaultValue) {
        return conf.getInt(key, defaultValue);
      }

      @Override
      protected String getTrimmed(String key, String defaultValue) {
        return conf.getTrimmed(key, defaultValue);
      }
    };

    final int handlerCount = get.ipc().handlers();
    final InetSocketAddress address = get.ipc().address();

    final BlockingService service
        = RaftServerProtocolService.newReflectiveBlockingService(
            new RaftServerProtocolServerSideTranslatorPB(serverProtocol));
    RPC.setProtocolEngine(conf, RaftServerProtocolPB.class, ProtobufRpcEngineShaded.class);
    return new RPC.Builder(conf)
        .setProtocol(RaftServerProtocolPB.class)
        .setInstance(service)
        .setBindAddress(address.getHostName())
        .setPort(address.getPort())
        .setNumHandlers(handlerCount)
        .setVerbose(false)
        .build();
  }

  private void addRaftClientProtocol(RaftClientProtocol clientProtocol, Configuration conf) {
    final Class<?> protocol = RaftClientProtocolPB.class;
    RPC.setProtocolEngine(conf,protocol, ProtobufRpcEngineShaded.class);

    final BlockingService service
        = RaftClientProtocolService.newReflectiveBlockingService(
        new RaftClientProtocolServerSideTranslatorPB(clientProtocol));
    ipcServer.addProtocol(RPC.RpcKind.RPC_PROTOCOL_BUFFER, protocol, service);
  }

  @Override
  public void start() {
    ipcServer.start();
  }

  @Override
  public void close() {
    ipcServer.stop();
  }

  @Override
  public AppendEntriesReplyProto appendEntries(
      AppendEntriesRequestProto request) throws IOException {
    Preconditions.checkArgument(id.equals(request.getServerRequest().getRequestorId()));
    CodeInjectionForTesting.execute(SEND_SERVER_REQUEST, id, null, request);

    final RaftServerProtocolPB proxy = proxies.getProxy(
        request.getServerRequest().getReplyId()).getProtocol();
    try {
      return proxy.appendEntries(null, request);
    } catch (ServiceException se) {
      throw ProtoUtils.toIOException(se);
    }
  }

  @Override
  public InstallSnapshotReplyProto installSnapshot(
      InstallSnapshotRequestProto request) throws IOException {
    Preconditions.checkArgument(id.equals(request.getServerRequest().getRequestorId()));
    CodeInjectionForTesting.execute(SEND_SERVER_REQUEST, id, null, request);

    final RaftServerProtocolPB proxy = proxies.getProxy(
        request.getServerRequest().getReplyId()).getProtocol();
    try {
      return proxy.installSnapshot(null, request);
    } catch (ServiceException se) {
      throw ProtoUtils.toIOException(se);
    }
  }

  @Override
  public RequestVoteReplyProto requestVote(
      RequestVoteRequestProto request) throws IOException {
    Preconditions.checkArgument(id.equals(request.getServerRequest().getRequestorId()));
    CodeInjectionForTesting.execute(SEND_SERVER_REQUEST, id, null, request);

    final RaftServerProtocolPB proxy = proxies.getProxy(
        request.getServerRequest().getReplyId()).getProtocol();
    try {
      return proxy.requestVote(null, request);
    } catch (ServiceException se) {
      throw ProtoUtils.toIOException(se);
    }
  }

  @Override
  public void addPeers(Iterable<RaftPeer> peers) {
    proxies.addPeers(peers);
  }
}
