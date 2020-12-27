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
import org.apache.hadoop.ipc.ProtobufRpcEngine;
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
import org.apache.ratis.proto.RaftProtos.StartLeaderElectionReplyProto;
import org.apache.ratis.proto.RaftProtos.StartLeaderElectionRequestProto;
import org.apache.ratis.proto.hadoop.HadoopCompatibilityProtos.HadoopServerProtocolService;
import org.apache.ratis.proto.hadoop.HadoopCompatibilityProtos.HadoopClientProtocolService;
import org.apache.ratis.proto.hadoop.HadoopCompatibilityProtos.ServerOps;
import org.apache.ratis.proto.hadoop.HadoopCompatibilityProtos.ServerRequestProto;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerRpcWithProxy;
import org.apache.ratis.server.protocol.RaftServerProtocol;
import com.google.protobuf.BlockingService;
import com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.GeneratedMessageV3;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.util.CodeInjectionForTesting;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.PeerProxyMap;
import org.apache.ratis.util.function.CheckedFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

/** Server side Hadoop RPC service. */
public final class HadoopRpcService extends RaftServerRpcWithProxy<Proxy<RaftServerProtocolPB>,
    PeerProxyMap<Proxy<RaftServerProtocolPB>>> {
  public static final Logger LOG = LoggerFactory.getLogger(HadoopRpcService.class);
  static final String CLASS_NAME = JavaUtils.getClassSimpleName(HadoopRpcService.class);
  public static final String SEND_SERVER_REQUEST = CLASS_NAME + ".sendServerRequest";

  public static final class Builder {
    private RaftServer server;
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

    public Builder setServer(RaftServer raftServer) {
      this.server = raftServer;
      return this;
    }

    public HadoopRpcService build() {
      return new HadoopRpcService(server, getConf());
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

    LOG.info(JavaUtils.getClassSimpleName(getClass()) + " created RPC.Server at " + ipcServerAddress);
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
        = HadoopServerProtocolService.newReflectiveBlockingService(
            new RaftServerProtocolServerSideTranslatorPB(serverProtocol));
    RPC.setProtocolEngine(conf, RaftServerProtocolPB.class, ProtobufRpcEngine.class);
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
    RPC.setProtocolEngine(conf, protocol, ProtobufRpcEngine.class);

    final BlockingService service = HadoopClientProtocolService.newReflectiveBlockingService(
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
        ServerOps.appendEntries,
        AppendEntriesReplyProto::parseFrom);
  }

  @Override
  public InstallSnapshotReplyProto installSnapshot(
      InstallSnapshotRequestProto request) throws IOException {
    return processRequest(request, request.getServerRequest().getReplyId(),
        ServerOps.installSnapshot, InstallSnapshotReplyProto::parseFrom);
  }

  @Override
  public RequestVoteReplyProto requestVote(
      RequestVoteRequestProto request) throws IOException {
    return processRequest(request, request.getServerRequest().getReplyId(),
        ServerOps.requestVote, RequestVoteReplyProto::parseFrom);
  }

  @Override
  public StartLeaderElectionReplyProto startLeaderElection(
      StartLeaderElectionRequestProto request) throws IOException {
    return processRequest(request, request.getServerRequest().getReplyId(),
        ServerOps.startLeaderElection, StartLeaderElectionReplyProto::parseFrom);
  }

  private <REQUEST extends GeneratedMessageV3, REPLY> REPLY processRequest(
      REQUEST request, org.apache.ratis.thirdparty.com.google.protobuf.ByteString replyId,
      ServerOps type, CheckedFunction<byte[], REPLY, InvalidProtocolBufferException> func)
      throws IOException {
    CodeInjectionForTesting.execute(SEND_SERVER_REQUEST, getId(), null, request);
    ServerRequestProto p = ServerRequestProto.newBuilder()
        .setRequest(ByteString.copyFrom(request.toByteArray()))
        .setType(type)
        .build();

    final RaftServerProtocolPB proxy = getProxies().getProxy(
        RaftPeerId.valueOf(replyId)).getProtocol();
    try {
      byte[] replyBytes = proxy.sendServer(null, p).getResponse().toByteArray();
      return func.apply(replyBytes);
    } catch (Exception se) {
      throw new IOException(se);
    }
  }
}
