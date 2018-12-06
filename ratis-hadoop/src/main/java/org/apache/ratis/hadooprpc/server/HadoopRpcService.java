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
package org.apache.ratis.hadooprpc.server;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngineShaded;
import org.apache.hadoop.ipc.RPC;
import org.apache.ratis.hadooprpc.HadoopConfigKeys;
import org.apache.ratis.hadooprpc.Proxy;
import org.apache.ratis.hadooprpc.client.CombinedClientProtocolPB;
import org.apache.ratis.hadooprpc.client.CombinedClientProtocolServerSideTranslatorPB;
import org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotReplyProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotRequestProto;
import org.apache.ratis.proto.RaftProtos.RequestVoteReplyProto;
import org.apache.ratis.proto.RaftProtos.RequestVoteRequestProto;
import org.apache.ratis.proto.hadoop.HadoopProtos.CombinedClientProtocolService;
import org.apache.ratis.proto.hadoop.HadoopProtos.RaftServerProtocolService;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerRpc;
import org.apache.ratis.server.impl.RaftServerRpcWithProxy;
import org.apache.ratis.server.protocol.RaftServerProtocol;
import org.apache.ratis.thirdparty.com.google.protobuf.BlockingService;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.ServiceException;
import org.apache.ratis.util.CodeInjectionForTesting;
import org.apache.ratis.util.PeerProxyMap;
import org.apache.ratis.util.ProtoUtils;
import org.apache.ratis.util.function.CheckedFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

/** Server side Hadoop RPC service. */
public class HadoopRpcService extends RaftServerRpcWithProxy<Proxy<RaftServerProtocolPB>, PeerProxyMap<Proxy<RaftServerProtocolPB>>> {
  public static final Logger LOG = LoggerFactory.getLogger(HadoopRpcService.class);
  static final String CLASS_NAME = HadoopRpcService.class.getSimpleName();
  public static final String SEND_SERVER_REQUEST = CLASS_NAME + ".sendServerRequest";

  public static class Builder extends RaftServerRpc.Builder<Builder, HadoopRpcService> {
    private Configuration conf;

    private Builder() {}

    public Configuration getConf() {
      if (conf == null) {
        conf = new Configuration();
      }
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
    public HadoopRpcService build() {
      return new HadoopRpcService(getServer(), getConf());
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  private final RPC.Server ipcServer;
  private final InetSocketAddress ipcServerAddress;

  private HadoopRpcService(RaftServer server, final Configuration conf) {
    super(server::getId,
        id -> new PeerProxyMap<>(id.toString(),
            p -> new Proxy<>(RaftServerProtocolPB.class, p.getAddress(), conf)));
    try {
      this.ipcServer = newRpcServer(server, conf);
    } catch (IOException e) {
      throw new RuntimeException("Failed to create Hadoop rpc server.", e);
    }
    this.ipcServerAddress = ipcServer.getListenerAddress();

    addRaftClientProtocol(server, conf);

    LOG.info(getClass().getSimpleName() + " created RPC.Server at "
        + ipcServerAddress);
  }

  @Override
  public SupportedRpcType getRpcType() {
    return SupportedRpcType.HADOOP;
  }

  @Override
  public InetSocketAddress getInetSocketAddress() {
    return ipcServerAddress;
  }

  private static RPC.Server newRpcServer(
      RaftServerProtocol serverProtocol, final Configuration conf)
      throws IOException {
    final int handlerCount = HadoopConfigKeys.Ipc.handlers(conf);
    final InetSocketAddress address = HadoopConfigKeys.Ipc.address(conf);

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

  private void addRaftClientProtocol(RaftServer server, Configuration conf) {
    final Class<?> protocol = CombinedClientProtocolPB.class;
    RPC.setProtocolEngine(conf, protocol, ProtobufRpcEngineShaded.class);

    final BlockingService service = CombinedClientProtocolService.newReflectiveBlockingService(
        new CombinedClientProtocolServerSideTranslatorPB(server));
    ipcServer.addProtocol(RPC.RpcKind.RPC_PROTOCOL_BUFFER, protocol, service);
  }

  @Override
  public void startImpl() {
    ipcServer.start();
  }

  @Override
  public void closeImpl() throws IOException {
    ipcServer.stop();
    super.closeImpl();
  }

  @Override
  public AppendEntriesReplyProto appendEntries(
      AppendEntriesRequestProto request) throws IOException {
    return processRequest(request, request.getServerRequest().getReplyId(),
        proxy -> proxy.appendEntries(null, request));
  }

  @Override
  public InstallSnapshotReplyProto installSnapshot(
      InstallSnapshotRequestProto request) throws IOException {
    return processRequest(request, request.getServerRequest().getReplyId(),
        proxy -> proxy.installSnapshot(null, request));
  }

  @Override
  public RequestVoteReplyProto requestVote(
      RequestVoteRequestProto request) throws IOException {
    return processRequest(request, request.getServerRequest().getReplyId(),
        proxy -> proxy.requestVote(null, request));
  }

  private <REQUEST, REPLY> REPLY processRequest(
      REQUEST request, ByteString replyId,
      CheckedFunction<RaftServerProtocolPB, REPLY, ServiceException> f)
      throws IOException {
    CodeInjectionForTesting.execute(SEND_SERVER_REQUEST, getId(), null, request);

    final RaftServerProtocolPB proxy = getProxies().getProxy(
        RaftPeerId.valueOf(replyId)).getProtocol();
    try {
      return f.apply(proxy);
    } catch (ServiceException se) {
      throw ProtoUtils.toIOException(se);
    }
  }
}
