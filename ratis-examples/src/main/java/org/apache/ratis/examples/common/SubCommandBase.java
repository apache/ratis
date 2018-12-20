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
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;

import java.util.stream.Stream;

/**
 * Base subcommand class which includes the basic raft properties.
 */
public abstract class SubCommandBase {

  @Parameter(names = {"--raftGroup",
      "-g"}, description = "Raft group identifier")
  protected String raftGroupId = "demoRaftGroup123";

  @Parameter(names = {"--peers", "-r"}, description =
      "Raft peers (format: name:host:port,"
          + "name:host:port)", required = true)
  protected String peers;

  public static RaftPeer[] parsePeers(String peers) {
    return Stream.of(peers.split(",")).map(address -> {
      String[] addressParts = address.split(":");
      return new RaftPeer(RaftPeerId.valueOf(addressParts[0]),
          addressParts[1] + ":" + addressParts[2]);
    }).toArray(RaftPeer[]::new);
  }

  public RaftPeer[] getPeers() {
    return parsePeers(peers);
  }

  public abstract void run() throws Exception;
}