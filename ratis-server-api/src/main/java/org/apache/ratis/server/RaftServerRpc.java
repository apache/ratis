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
package org.apache.ratis.server;

import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.server.protocol.RaftServerAsynchronousProtocol;
import org.apache.ratis.server.protocol.RaftServerProtocol;
import org.apache.ratis.util.JavaUtils;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * An server-side interface for supporting different RPC implementations
 * such as Netty, gRPC and Hadoop.
 */
public interface RaftServerRpc extends RaftServerProtocol, RpcType.Get, RaftPeer.Add, Closeable {
  /** Start the RPC service. */
  void start() throws IOException;

  /** @return the address where this RPC server is listening */
  InetSocketAddress getInetSocketAddress();

  /** @return the address where this RPC server is listening for client requests */
  default InetSocketAddress getClientServerAddress() {
    return getInetSocketAddress();
  }
  /** @return the address where this RPC server is listening for admin requests */
  default InetSocketAddress getAdminServerAddress() {
    return getInetSocketAddress();
  }

  /** Handle the given exception.  For example, try reconnecting. */
  void handleException(RaftPeerId serverId, Exception e, boolean reconnect);

  /** The server role changes from leader to a non-leader role. */
  default void notifyNotLeader(RaftGroupId groupId) {
  }

  default RaftServerAsynchronousProtocol async() {
    throw new UnsupportedOperationException(getClass().getName()
        + " does not support " + JavaUtils.getClassSimpleName(RaftServerAsynchronousProtocol.class));
  }
}
