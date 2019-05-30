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

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.logservice.api.LogServiceConfiguration;
import org.apache.ratis.logservice.common.Constants;
import org.apache.ratis.logservice.util.LogServiceUtils;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.NetUtils;
import org.apache.ratis.util.TimeDuration;

/**
 * A base class to encapsulate functionality around a long-lived Java process which runs a state machine.
 */
public abstract class BaseServer implements Closeable {

  private final ServerOpts opts;
  private final LogServiceConfiguration config;

  public BaseServer(ServerOpts opts) {
    this.opts = Objects.requireNonNull(opts);
    this.config = LogServiceConfiguration.create();
  }

  public ServerOpts getServerOpts() {
    return opts;
  }

  public LogServiceConfiguration getConfig() {
    return config;
  }
  /**
   * Sets common Ratis server properties for both the log and metadata state machines.
   */
  void setRaftProperties(RaftProperties properties) {
    // Set the ports for the server
    GrpcConfigKeys.Server.setPort(properties, opts.getPort());
    NettyConfigKeys.Server.setPort(properties, opts.getPort());

    // Ozone sets the leader election timeout (min) to 1second.
    long leaderElectionTimeoutMinVal = getLeaderElectionTimeoutMin();
    TimeDuration leaderElectionTimeoutMin = TimeDuration.valueOf(leaderElectionTimeoutMinVal,
      TimeUnit.MILLISECONDS);
    RaftServerConfigKeys.Rpc.setTimeoutMin(properties, leaderElectionTimeoutMin);
    long leaderElectionTimeoutMaxVal = getLeaderElectionTimeoutMax();

    TimeDuration leaderElectionMaxTimeout = TimeDuration.valueOf(
      leaderElectionTimeoutMaxVal,
        TimeUnit.MILLISECONDS);
    RaftServerConfigKeys.Rpc.setTimeoutMax(properties, leaderElectionMaxTimeout);
  }

  private long getLeaderElectionTimeoutMin() {
    return config.getLong(Constants.LOG_SERVICE_LEADER_ELECTION_TIMEOUT_MIN_KEY,
        Constants.DEFAULT_LOG_SERVICE_LEADER_ELECTION_TIMEOUT_MIN);
  }

  private long getLeaderElectionTimeoutMax() {
    return config.getLong(Constants.LOG_SERVICE_LEADER_ELECTION_TIMEOUT_MAX_KEY,
        Constants.DEFAULT_LOG_SERVICE_LEADER_ELECTION_TIMEOUT_MAX);
  }

  /**
   * Validates that there are no properties set which are in conflict with the LogService.
   */
  void validateRaftProperties(RaftProperties properties) {
    if (RaftServerConfigKeys.Snapshot.autoTriggerEnabled(properties)) {
      throw new IllegalStateException("Auto triggering snapshots is disallowed by the LogService");
    }
  }

  static ServerOpts buildOpts(String hostname, String metaQuorum, int port, String workingDir) {
    ServerOpts opts = new ServerOpts();
    opts.setHost(hostname);
    opts.setMetaQuorum(metaQuorum);
    opts.setPort(port);
    opts.setWorkingDir(workingDir);
    return opts;
  }

  public abstract static class Builder<T extends BaseServer> {
    private ServerOpts opts = new ServerOpts();

    protected ServerOpts getOpts() {
      return opts;
    }

    public abstract T build();

    public Builder<T> validate() {
      if (!opts.isPortSet()) {
        InetSocketAddress addr = NetUtils.createLocalServerAddress();
        opts.setPort(addr.getPort());
      }
      if (!opts.isHostSet()) {
        opts.setHost(LogServiceUtils.getHostName());
      }
      if (!opts.isWorkingDirSet()) {
        throw new IllegalArgumentException("Working directory was not specified");
      }
      return this;
    }

    public Builder<T> setMetaQuorum(String meta) {
        opts.setMetaQuorum(meta);
        return this;
    }

    public Builder<T> setPort(int port) {
        opts.setPort(port);
        return this;
    }

    public Builder<T> setWorkingDir(String workingDir) {
        opts.setWorkingDir(workingDir);
        return this;
    }

    public Builder<T> setHostName(String hostName) {
      opts.setHost(hostName);
      return this;
    }
  }
}
