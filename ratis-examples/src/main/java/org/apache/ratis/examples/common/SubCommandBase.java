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
package org.apache.ratis.examples.common;

import com.beust.jcommander.Parameter;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base subcommand class which includes the basic raft properties.
 */
public abstract class SubCommandBase {

  private static final Logger LOG = LoggerFactory.getLogger(SubCommandBase.class);

  static Pattern IPV6PATTERN = Pattern.compile(Stream.of("(",
    "([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|",          // 2001:0250:0207:0001:0000:0000:0000:ff02
    "([0-9a-fA-F]{1,4}:){1,7}:|",                         // 1::
    "([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|",         // 2001::ff02
    "([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|",  // 2001::0000:ff02
    "([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|",  // 2001::0000:0000:ff02
    "([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|",  // 2001::0000:0000:0000:ff02
    "([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|",  // 2001::0001:0000:0000:0000:ff02
    "[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|",       // 2001::0207:0001:0000:0000:0000:ff02
    ":((:[0-9a-fA-F]{1,4}){1,7}|:)|",                     // ::0250:0207:0001:0000:0000:0000:ff02
    "::(ffff(:0{1,4}){0,1}:){0,1}",
    "((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\\.){3,3}",
    "(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|",          // ::ffff:255.255.255.255 ::ffff:0:255.255.255.255
    "([0-9a-fA-F]{1,4}:){1,4}:",
    "((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\\.){3,3}",
    "(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])",           // 2001:db8:3:4::192.0.2.33  64:ff9b::192.0.2.33
    ")").collect(Collectors.joining()));

  @Parameter(names = {"--raftGroup",
      "-g"}, description = "Raft group identifier")
  private String raftGroupId = "demoRaftGroup123";

  @Parameter(names = {"--peers", "-r"}, description =
      "Raft peers (format: name:host:port,"
          + "name:host:port)", required = true)
  private String peers;

  public static RaftPeer[] parsePeers(String peers) {
    return Stream.of(peers.split(",")).map(address -> parseRaftPeer(address))
      .toArray(RaftPeer[]::new);
  }

  public RaftPeer[] getPeers() {
    return parsePeers(peers);
  }

  public abstract void run() throws Exception;

  public String getRaftGroupId() {
    return raftGroupId;
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

  private static RaftPeer parseRaftPeer(String address) {
    try {
      if (IPV6PATTERN.matcher(address).find()) {
        String id = address.substring(0, address.indexOf(":"));
        String host = address.substring(address.indexOf("[") + 1, address.lastIndexOf("]"));
        String port = address.substring(address.lastIndexOf(":") + 1);
        return new RaftPeer(RaftPeerId.valueOf(id),
          new InetSocketAddress(Inet6Address.getByName(host), Integer.valueOf(port)));
      } else {
        String[] addressParts = address.split(":");
        return new RaftPeer(RaftPeerId.valueOf(addressParts[0]),
          new InetSocketAddress(Inet4Address.getByAddress(addressParts[1].getBytes()),
            Integer.valueOf(addressParts[2])));
      }
    } catch (UnknownHostException ex) {
      LOG.error("parse address {} failed",address);
      throw new RuntimeException(ex);
    }
  }
}
