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
package org.apache.raft.hadoopRpc.server;

import com.google.common.base.Preconditions;
import com.google.protobuf.BlockingService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.raft.hadoopRpc.HadoopRpcBase;
import org.apache.raft.hadoopRpc.HadoopUtils;
import org.apache.raft.hadoopRpc.client.RaftClientProtocolPB;
import org.apache.raft.hadoopRpc.client.RaftClientProtocolServerSideTranslatorPB;
import org.apache.raft.proto.RaftClientProtocolProtos.RaftClientProtocolService;
import org.apache.raft.proto.RaftServerProtocolProtos.RaftServerProtocolService;
import org.apache.raft.protocol.RaftClientReply;
import org.apache.raft.protocol.RaftClientRequest;
import org.apache.raft.protocol.RaftPeer;
import org.apache.raft.server.RaftServer;
import org.apache.raft.server.RaftServerConfigKeys;
import org.apache.raft.server.RaftServerRpc;
import org.apache.raft.server.protocol.AppendEntriesRequest;
import org.apache.raft.server.protocol.InstallSnapshotRequest;
import org.apache.raft.server.protocol.RaftServerReply;
import org.apache.raft.server.protocol.RaftServerRequest;
import org.apache.raft.server.protocol.RequestVoteRequest;
import org.apache.raft.util.CodeInjectionForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

/** Server side Hadoop RPC service. */
public class HadoopRpcService
    extends HadoopRpcBase<RaftServerProtocolClientSideTranslatorPB>
    implements RaftServerRpc {
  public static final Logger LOG = LoggerFactory.getLogger(HadoopRpcService.class);
  static final String CLASS_NAME = HadoopRpcService.class.getSimpleName();
  public static final String SEND_SERVER_REQUEST = CLASS_NAME + ".sendServerRequest";

  private final RaftServerRpcService raftService;
  private final RPC.Server ipcServer;
  private final InetSocketAddress ipcServerAddress;

  public HadoopRpcService(RaftServer server, Configuration conf)
      throws IOException {
    super(conf);
    this.raftService = new RaftServerRpcService(server);
    this.ipcServer = newRpcServer(conf);
    this.ipcServerAddress = ipcServer.getListenerAddress();

    addRaftClientProtocol(conf);

    LOG.info(getClass().getSimpleName() + " created RPC.Server at "
        + ipcServerAddress);
  }

  @Override
  public InetSocketAddress getInetSocketAddress() {
    return ipcServerAddress;
  }

  @Override
  public RaftServerProtocolClientSideTranslatorPB createProxy(RaftPeer p)
      throws IOException {
    final RaftServerProtocolPB proxy = HadoopUtils.getProxy(
        RaftServerProtocolPB.class, p.getAddress(), getConf());
    return new RaftServerProtocolClientSideTranslatorPB(proxy);
  }

  RPC.Server newRpcServer(Configuration conf) throws IOException {
    final RaftServerConfigKeys.Get get = new RaftServerConfigKeys.Get(conf);
    final int handlerCount = get.ipc().handlers();
    final InetSocketAddress address = get.ipc().address();

    final BlockingService service
        = RaftServerProtocolService.newReflectiveBlockingService(
            new RaftServerProtocolServerSideTranslatorPB(raftService));
    HadoopUtils.setProtobufRpcEngine(RaftServerProtocolPB.class, conf);
    return new RPC.Builder(conf)
        .setProtocol(RaftServerProtocolPB.class)
        .setInstance(service)
        .setBindAddress(address.getHostName())
        .setPort(address.getPort())
        .setNumHandlers(handlerCount)
        .setVerbose(false)
        .build();
  }

  private void addRaftClientProtocol(Configuration conf) {
    final Class<?> protocol = RaftClientProtocolPB.class;
    HadoopUtils.setProtobufRpcEngine(protocol, conf);

    final BlockingService service
        = RaftClientProtocolService.newReflectiveBlockingService(
        new RaftClientProtocolServerSideTranslatorPB(raftService));
    ipcServer.addProtocol(RPC.RpcKind.RPC_PROTOCOL_BUFFER, protocol, service);
  }

  @Override
  public void start() {
    ipcServer.start();
  }

  @Override
  public void interruptAndJoin() throws InterruptedException {
    // nothing to do.
  }

  @Override
  public void shutdown() {
    ipcServer.stop();
  }

  @Override
  public RaftServerReply sendServerRequest(RaftServerRequest request)
      throws IOException {
    Preconditions.checkArgument(
        raftService.getId().equals(request.getRequestorId()));
    CodeInjectionForTesting.execute(SEND_SERVER_REQUEST, raftService.getId(),
        null, request);

    final String id = request.getReplierId();
    final RaftServerProtocolClientSideTranslatorPB proxy = getServerProxy(id);

    if (request instanceof AppendEntriesRequest) {
      return proxy.appendEntries((AppendEntriesRequest)request);
    } else if (request instanceof RequestVoteRequest) {
      return proxy.requestVote((RequestVoteRequest) request);
    } else if (request instanceof InstallSnapshotRequest) {
      return proxy.installSnapshot((InstallSnapshotRequest) request);
    } else {
      throw new UnsupportedOperationException("Unsupported request "
          + request.getClass() + ", " + request);
    }
  }

  @Override
  public void addPeerProxies(Iterable<RaftPeer> peers) {
    addPeers(peers);
  }
}
