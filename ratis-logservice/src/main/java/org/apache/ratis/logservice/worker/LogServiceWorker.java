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

package org.apache.ratis.logservice.worker;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.logservice.server.LogStateMachine;
import org.apache.ratis.logservice.server.ManagementStateMachine;
import org.apache.ratis.logservice.util.MetaServiceProtoUtil;
import org.apache.ratis.logservice.util.LogServiceUtils;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.NetUtils;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Set;

import static org.apache.ratis.logservice.common.Constants.metaGroupID;
import static org.apache.ratis.logservice.common.Constants.serversGroupID;

public class LogServiceWorker implements Cloneable{

    @Parameter(names = "-port", description = "Port number")
    private int port;

    @Parameter(names = "-dir", description = "Working directory")
    private  String workingDir;

    @Parameter(names = "-meta", description = "Meta Quorum ID")
    private  String metaIdentity;
    RaftServer raftServer = null;
    RaftClient metaClient = null;

    public LogServiceWorker() {

    }
    public LogServiceWorker(String meta, int port, String workingDir) {
        this.metaIdentity = meta;
        this.port = port;
        this.workingDir = workingDir;
    }

    public RaftServer getServer() {
        return raftServer;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public void start() throws IOException {
        Set<RaftPeer> peers = LogServiceUtils.getPeersFromQuorum(metaIdentity);
        String host = LogServiceUtils.getHostName();
        RaftProperties properties = new RaftProperties();
        properties.set("raft.client.rpc.request.timeout", "100000");
        GrpcConfigKeys.Server.setPort(properties, port);
        NettyConfigKeys.Server.setPort(properties, port);
        InetSocketAddress addr = new InetSocketAddress(host,port);
        if(workingDir != null) {
            RaftServerConfigKeys.setStorageDirs(properties, Collections.singletonList(new File(workingDir)));
        }
        String id = host +"_" +  port;
        RaftPeer peer = new RaftPeer(RaftPeerId.valueOf(id), addr);
        RaftGroup all = RaftGroup.valueOf(serversGroupID, peer);
        RaftGroup meta = RaftGroup.valueOf(metaGroupID, peers);
        raftServer = RaftServer.newBuilder()
                .setStateMachineRegistry(new StateMachine.Registry() {
                    final StateMachine managementMachine = new ManagementStateMachine();
                    final StateMachine logMachine  = new LogStateMachine();
                    @Override
                    public StateMachine apply(RaftGroupId raftGroupId) {
                        if(raftGroupId.equals(serversGroupID)) {
                            return managementMachine;
                        }
                        return logMachine;
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
        LogServiceWorker worker = new LogServiceWorker();
        JCommander.newBuilder()
                .addObject(worker)
                .build()
                .parse(args);
        worker.start();


    }


    public void close() throws IOException {
        raftServer.close();
    }

    public static class Builder {
        String meta;
        int port = -1;
        private String workingDir;

        public LogServiceWorker build() {
            if(port == -1) {
                InetSocketAddress addr = NetUtils.createLocalServerAddress();
                port = addr.getPort();
            }
            return new LogServiceWorker(meta, port, workingDir);
        }
        public Builder setMetaIdentity(String meta) {
            this.meta = meta;
            return this;
        }
        public Builder setPort(int port) {
            this.port = port;
            return this;
        }

        public Builder setWorkingDir(String workingDir) {
            this.workingDir = workingDir;
            return this;
        }


    }
}
