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

package org.apache.ratis.logservice.server;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Set;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.logservice.util.LogServiceUtils;
import org.apache.ratis.logservice.util.MetaServiceProtoUtil;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.statemachine.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;

public class LogServer extends BaseServer {
    private static final Logger LOG = LoggerFactory.getLogger(LogServer.class);

    private RaftServer raftServer = null;
    private RaftClient metaClient = null;

    public LogServer(ServerOpts opts) {
      super(opts);
    }

    public RaftServer getServer() {
        return raftServer;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public void start() throws IOException {
        final ServerOpts opts = getServerOpts();
        Set<RaftPeer> peers = LogServiceUtils.getPeersFromQuorum(opts.getMetaQuorum());
        RaftProperties properties = new RaftProperties();
        properties.set("raft.client.rpc.request.timeout", "100000");
        GrpcConfigKeys.Server.setPort(properties, opts.getPort());
        NettyConfigKeys.Server.setPort(properties, opts.getPort());
        InetSocketAddress addr = new InetSocketAddress(opts.getHost(), opts.getPort());
        if(opts.getWorkingDir() != null) {
            RaftServerConfigKeys.setStorageDirs(properties, Collections.singletonList(new File(opts.getWorkingDir())));
        }
        String id = opts.getHost() +"_" +  opts.getPort();
        RaftPeer peer = new RaftPeer(RaftPeerId.valueOf(id), addr);
        final RaftGroupId logServerGroupId = RaftGroupId.valueOf(opts.getLogServerGroupId());
        RaftGroup all = RaftGroup.valueOf(logServerGroupId, peer);
        RaftGroup meta = RaftGroup.valueOf(RaftGroupId.valueOf(opts.getMetaGroupId()), peers);
        raftServer = RaftServer.newBuilder()
                .setStateMachineRegistry(new StateMachine.Registry() {
                    @Override
                    public StateMachine apply(RaftGroupId raftGroupId) {
                        // TODO this looks wrong. Why isn't this metaGroupId?
                        if(raftGroupId.equals(logServerGroupId)) {
                            return new ManagementStateMachine();
                        }
                        return new LogStateMachine();
                    }
                })
                .setProperties(properties)
                .setServerId(RaftPeerId.valueOf(id))
                .setGroup(all)
                .build();
        raftServer.start();

        metaClient = RaftClient.newBuilder()
                .setRaftGroup(meta)
                .setClientId(ClientId.randomId())
                .setProperties(properties)
                .build();
        metaClient.send(() -> MetaServiceProtoUtil.toPingRequestProto(peer).toByteString());
    }

    public static void main(String[] args) throws IOException {
        ServerOpts opts = new ServerOpts();
        JCommander.newBuilder()
                .addObject(opts)
                .build()
                .parse(args);

        try (LogServer worker = new LogServer(opts)) {
          worker.start();
          while (true) {
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              return;
            }
          }
        }
    }


    public void close() throws IOException {
        raftServer.close();
    }

    public static class Builder extends BaseServer.Builder<LogServer> {
        public LogServer build() {
            validate();
            return new LogServer(getOpts());
        }
    }
}
