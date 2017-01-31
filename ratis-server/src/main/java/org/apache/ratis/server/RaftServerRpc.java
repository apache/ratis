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
package org.apache.ratis.server;

import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.protocol.RaftServerProtocol;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * An server-side interface for supporting different RPC implementations
 * such as Netty, gRPC and Hadoop.
 */
public interface RaftServerRpc extends RaftServerProtocol, Closeable {
  /** To build {@link RaftServerRpc} objects. */
  abstract class Builder<B extends Builder, RPC extends RaftServerRpc> {
    private RaftServer server;
    private int port;

    /** Construct a builder with the default port. */
    protected Builder(int defaultPort) {
      this.port = defaultPort;
    }

    public RaftServer getServer() {
      return server;
    }

    public B setServer(RaftServer server) {
      this.server = server;
      return getThis();
    }

    public int getPort() {
      return port;
    }

    /** Set the port for the server to listen to. */
    public B setPort(int port) {
      this.port = port;
      return getThis();
    }

    protected abstract B getThis();

    public abstract RPC build() throws IOException;
  }

  /** Start the RPC service. */
  void start();

  /** @return the address where this RPC server is listening to. */
  InetSocketAddress getInetSocketAddress();

  /** add information of the given peers */
  void addPeers(Iterable<RaftPeer> peers);
}
