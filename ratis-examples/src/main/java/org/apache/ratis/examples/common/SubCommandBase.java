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
package org.apache.ratis.examples.common;

import com.beust.jcommander.Parameter;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.RoutingTable;

import java.util.Collection;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Base subcommand class which includes the basic raft properties.
 */
public abstract class SubCommandBase {

  @Parameter(names = {"--raftGroup",
      "-g"}, description = "Raft group identifier")
  private String raftGroupId = "demoRaftGroup123";

  @Parameter(names = {"--peers", "-r"}, description =
      "Raft peers (format: name:host:port:dataStreamPort:clientPort:adminPort,"
          + "...)", required = true)
  private String peers;

  public static RaftPeer[] parsePeers(String peers) {
    return Stream.of(peers.split(",")).map(SubCommandBase::parsePeer).toArray(RaftPeer[]::new);
  }

  /**
   * Parse a single peer definition in the format
   * {@code name:host:port:dataStreamPort:clientPort:adminPort}, where the trailing
   * ports are optional.  The host may be an IPv6 literal enclosed in brackets,
   * e.g. {@code n0:[::1]:9000:9001:9002:9003}.
   */
  private static RaftPeer parsePeer(String address) {
    final int idEnd = address.indexOf(':');
    if (idEnd < 0) {
      throw illegalFormat(address);
    }
    final String id = address.substring(0, idEnd);
    final String hostAndPorts = address.substring(idEnd + 1);

    // Separate the host from the port list, honoring IPv6 bracketed literals.
    final String host;
    final String portList;
    if (hostAndPorts.startsWith("[")) {
      final int bracketEnd = hostAndPorts.indexOf("]:");
      if (bracketEnd < 0) {
        throw illegalFormat(address);
      }
      host = hostAndPorts.substring(0, bracketEnd + 1); // include the closing ']'
      portList = hostAndPorts.substring(bracketEnd + 2);
    } else {
      final int hostEnd = hostAndPorts.indexOf(':');
      if (hostEnd < 0) {
        throw illegalFormat(address);
      }
      host = hostAndPorts.substring(0, hostEnd);
      portList = hostAndPorts.substring(hostEnd + 1);
    }

    final String[] ports = portList.split(":");
    if (ports[0].isEmpty()) {
      throw illegalFormat(address);
    }
    final RaftPeer.Builder builder = RaftPeer.newBuilder();
    builder.setId(id).setAddress(host + ":" + ports[0]);
    if (ports.length >= 2) {
      builder.setDataStreamAddress(host + ":" + ports[1]);
    }
    if (ports.length >= 3) {
      builder.setClientAddress(host + ":" + ports[2]);
    }
    if (ports.length >= 4) {
      builder.setAdminAddress(host + ":" + ports[3]);
    }
    return builder.build();
  }

  private static IllegalArgumentException illegalFormat(String address) {
    return new IllegalArgumentException("Raft peer " + address + " is not a legitimate format. "
        + "(format: name:host:port:dataStreamPort:clientPort:adminPort)");
  }

  public RaftPeer[] getPeers() {
    return parsePeers(peers);
  }

  public RaftPeer getPrimary() {
    return parsePeers(peers)[0];
  }

  public abstract void run() throws Exception;

  public String getRaftGroupId() {
    return raftGroupId;
  }

  public RoutingTable getRoutingTable(Collection<RaftPeer> raftPeers, RaftPeer primary) {
    RoutingTable.Builder builder = RoutingTable.newBuilder();
    RaftPeer previous = primary;
    for (RaftPeer peer : raftPeers) {
      if (peer.equals(primary)) {
        continue;
      }
      builder.addSuccessor(previous.getId(), peer.getId());
      previous = peer;
    }

    return builder.build();
  }

  /**
   * @return the peer with the given id if it is in this group; otherwise, return null.
   */
  public RaftPeer getPeer(RaftPeerId raftPeerId) {
    Objects.requireNonNull(raftPeerId, "raftPeerId == null");
    for (RaftPeer p : getPeers()) {
      if (raftPeerId.equals(p.getId())) {
        return p;
      }
    }
    throw new IllegalArgumentException("Raft peer id " + raftPeerId + " is not part of the raft group definitions " +
            this.peers);
  }
}