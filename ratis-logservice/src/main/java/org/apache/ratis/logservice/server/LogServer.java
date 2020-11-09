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
import java.util.concurrent.TimeUnit;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.logservice.api.LogServiceConfiguration;
import org.apache.ratis.logservice.common.Constants;
import org.apache.ratis.logservice.util.LogServiceUtils;
import org.apache.ratis.logservice.util.MetaServiceProtoUtil;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.Daemon;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;

public class LogServer extends BaseServer {
    private static final Logger LOG = LoggerFactory.getLogger(LogServer.class);

    private RaftServer raftServer = null;
    private RaftClient metaClient = null;

    private Daemon daemon =  null;
    private long heartbeatInterval = Constants.DEFAULT_HEARTBEAT_INTERVAL;
    public LogServer(ServerOpts opts) {
      super(opts);
      LOG.debug("Log Server options: {}", opts);
    }

    public RaftServer getServer() {
        return raftServer;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    @Override
    void setRaftProperties(RaftProperties properties) {
      super.setRaftProperties(properties);

      // Increase the client timeout
      long rpcTimeout = getConfig().getLong(Constants.LOG_SERVICE_RPC_TIMEOUT_KEY,
        Constants.DEFAULT_RPC_TIMEOUT);
      RaftClientConfigKeys.Rpc.setRequestTimeout(properties,
        TimeDuration.valueOf(rpcTimeout, TimeUnit.MILLISECONDS));

      // Increase the segment size to avoid rolling so quickly
      long segmentSize = getConfig().getLong(Constants.RATIS_RAFT_SEGMENT_SIZE_KEY,
        Constants.DEFAULT_RATIS_RAFT_SEGMENT_SIZE);
      SizeInBytes segmentSizeBytes = SizeInBytes.valueOf(segmentSize);
      String archiveLocation = getConfig().get(Constants.LOG_SERVICE_ARCHIVAL_LOCATION_KEY);
      if (archiveLocation != null) {
        properties.set(Constants.LOG_SERVICE_ARCHIVAL_LOCATION_KEY, archiveLocation);
      }
      heartbeatInterval = getConfig().getLong(Constants.LOG_SERVICE_HEARTBEAT_INTERVAL_KEY,
        Constants.DEFAULT_HEARTBEAT_INTERVAL);
      if(heartbeatInterval <= 0) {
          LOG.warn("Heartbeat interval configuration is invalid." +
                  " Setting default value "+ Constants.DEFAULT_HEARTBEAT_INTERVAL);
          heartbeatInterval = Constants.DEFAULT_HEARTBEAT_INTERVAL;
      }
      RaftServerConfigKeys.Log.setSegmentSizeMax(properties, segmentSizeBytes);
      RaftServerConfigKeys.Log.setPreallocatedSize(properties, segmentSizeBytes);

      // TODO this seems to cause errors, not sure if pushing Ratis too hard?
      // SizeInBytes writeBufferSize = SizeInBytes.valueOf("128KB");
      // RaftServerConfigKeys.Log.setWriteBufferSize(properties, writeBufferSize);
    }

    public void start() throws IOException {
        final ServerOpts opts = getServerOpts();
        Set<RaftPeer> peers = LogServiceUtils.getPeersFromQuorum(opts.getMetaQuorum());
        RaftProperties properties = new RaftProperties();

        // Set properties for the log server state machine
        setRaftProperties(properties);

        InetSocketAddress addr = new InetSocketAddress(opts.getHost(), opts.getPort());
        if(opts.getWorkingDir() != null) {
            RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(new File(opts.getWorkingDir())));
        }
        String id = opts.getHost() +"_" +  opts.getPort();
        final RaftPeer peer = RaftPeer.newBuilder().setId(id).setAddress(addr).build();
        final RaftGroupId logServerGroupId = RaftGroupId.valueOf(opts.getLogServerGroupId());
        RaftGroup all = RaftGroup.valueOf(logServerGroupId, peer);
        RaftGroup meta = RaftGroup.valueOf(RaftGroupId.valueOf(opts.getMetaGroupId()), peers);

        // Make sure that we aren't setting any invalid/harmful properties
        validateRaftProperties(properties);

        raftServer = RaftServer.newBuilder()
                .setStateMachineRegistry(new StateMachine.Registry() {
                    @Override
                    public StateMachine apply(RaftGroupId raftGroupId) {
                        // TODO this looks wrong. Why isn't this metaGroupId?
                        if(raftGroupId.equals(logServerGroupId)) {
                            return new ManagementStateMachine();
                        }
                        return new LogStateMachine(properties);
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
        metaClient.io().send(() -> MetaServiceProtoUtil.toPingRequestProto(peer).toByteString());
        daemon = new Daemon(new HeartbeatSender(RaftPeer.newBuilder().setId(raftServer.getId()).build()),
                "heartbeat-Sender"+raftServer.getId());
        daemon.start();
    }

    public static void main(String[] args) throws IOException {
        ServerOpts opts = new ServerOpts();
        JCommander.newBuilder()
                .addObject(opts)
                .build()
                .parse(args);
        // Add config from log service configuration file
        LogServiceConfiguration config = LogServiceConfiguration.create();
        opts = config.addLogServerOpts(opts);

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
        metaClient.close();
        raftServer.close();
        daemon.interrupt();
    }

    public static class Builder extends BaseServer.Builder<LogServer> {
        public LogServer build() {
            validate();
            return new LogServer(getOpts());
        }
    }

    private class HeartbeatSender implements Runnable {

        private RaftPeer peer;
        HeartbeatSender(RaftPeer peer) {
            this.peer = peer;
        }

        @Override
        public void run() {

            while (true) {
                try {
                    metaClient.io().send(() -> MetaServiceProtoUtil.
                            toHeartbeatRequestProto(peer).toByteString());
                    Thread.sleep(heartbeatInterval);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (IOException e) {
                    LOG.warn("Heartbeat request failed with exception", e);
                }
            }

        }
    }
}
