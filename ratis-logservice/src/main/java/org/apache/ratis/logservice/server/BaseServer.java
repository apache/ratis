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

import org.apache.ratis.logservice.util.LogServiceUtils;
import org.apache.ratis.util.NetUtils;

/**
 * A base class to encapsulate functionality around a long-lived Java process which runs a state machine.
 */
public abstract class BaseServer implements Closeable {

  private final ServerOpts opts;

  public BaseServer(ServerOpts opts) {
    this.opts = Objects.requireNonNull(opts);
  }

  public ServerOpts getServerOpts() {
    return opts;
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
      if (opts.getPort() == -1) {
        InetSocketAddress addr = NetUtils.createLocalServerAddress();
        opts.setPort(addr.getPort());
      }
      if (opts.getHost() == null) {
        opts.setHost(LogServiceUtils.getHostName());
      }
      if (opts.getWorkingDir() == null) {
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
