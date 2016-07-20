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
package org.apache.hadoop.raft.hadoopRpc;

import com.google.protobuf.BlockingService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.raft.proto.RaftServerProtocolProtos.RaftServerProtocolService;
import org.apache.hadoop.raft.protocol.RaftClientReply;
import org.apache.hadoop.raft.protocol.RaftClientRequest;
import org.apache.hadoop.raft.protocol.RaftPeer;
import org.apache.hadoop.raft.server.PendingRequest;
import org.apache.hadoop.raft.server.RaftServer;
import org.apache.hadoop.raft.server.RaftServerConfigKeys;
import org.apache.hadoop.raft.server.RaftServerRpc;
import org.apache.hadoop.raft.server.protocol.*;
import org.apache.hadoop.raft.server.protocol.pb.RaftServerProtocolClientSideTranslatorPB;
import org.apache.hadoop.raft.server.protocol.pb.RaftServerProtocolPB;
import org.apache.hadoop.raft.server.protocol.pb.RaftServerProtocolServerSideTranslatorPB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HadoopRpcService implements RaftServerRpc {
  public static final Logger LOG = LoggerFactory.getLogger(HadoopRpcService.class);

  private final RaftServer server;
  private final RPC.Server ipcServer;
  private final InetSocketAddress ipcServerAddress;

  private final Map<String, RaftServerProtocolClientSideTranslatorPB> peers
      = new HashMap<>();

  public HadoopRpcService(RaftServer server, Configuration conf)
      throws IOException {
    this.server = server;
    this.ipcServerAddress = NetUtils.createSocketAddr(
        conf.getTrimmed(RaftServerConfigKeys.IPC_ADDRESS_KEY));
    this.ipcServer = newRpcServer(conf);

    LOG.info(getClass().getSimpleName() + " created RPC.Server at "
        + ipcServer.getListenerAddress());
  }

  void addPeers(List<RaftPeer> peers, Configuration conf) throws IOException {
    for(RaftPeer p : peers) {
      this.peers.put(p.getId(),
          new RaftServerProtocolClientSideTranslatorPB(p.getAddress(), conf));
    }
  }

  RPC.Server newRpcServer(Configuration conf) throws IOException {
    RPC.setProtocolEngine(conf, RaftServerProtocolPB.class,
        ProtobufRpcEngine.class);
    final int handlerCount = conf.getInt(
        RaftServerConfigKeys.HANDLER_COUNT_KEY,
        RaftServerConfigKeys.HANDLER_COUNT_DEFAULT);
    final BlockingService service
        = RaftServerProtocolService.newReflectiveBlockingService(
        new RaftServerProtocolServerSideTranslatorPB(server));
    return new RPC.Builder(conf)
        .setProtocol(RaftServerProtocolPB.class)
        .setInstance(service)
        .setBindAddress(ipcServerAddress.getHostName())
        .setPort(ipcServerAddress.getPort())
        .setNumHandlers(handlerCount)
        .setVerbose(false)
        .build();
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
    final String id = request.getReplierId();
    final RaftServerProtocolClientSideTranslatorPB proxy = peers.get(id);
    if (proxy == null) {
      throw new IllegalStateException("Raft server " + id + " not found.");
    }
    if (request instanceof AppendEntriesRequest) {
      return proxy.appendEntries((AppendEntriesRequest)request);
    } else if (request instanceof RequestVoteRequest) {
      return proxy.requestVote((RequestVoteRequest) request);
    } else {
      throw new UnsupportedOperationException("Unsupported request "
          + request.getClass() + ", " + request);
    }
  }

  @Override
  public void saveCallInfo(PendingRequest pending) throws IOException {
    // TODO do not wait and release handler immediately
    pending.waitForReply();
  }

  @Override
  public void sendClientReply(RaftClientRequest request, RaftClientReply reply,
                              IOException ioe) throws IOException {
    // TODO
  }
}
