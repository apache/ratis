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
package org.apache.ratis.shell.cli.sh.command;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.shell.cli.RaftUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Command for remove ratis server.
 */
public class QuorumRemoveCommand extends AbstractRatisCommand {
  public static final String REMOVE_PEER_ADDRESS_OPTION_NAME = "removePeer";

  /**
   * @param context command context
   */
  public QuorumRemoveCommand(Context context) {
    super(context);
  }

  @Override
  public String getCommandName() {
    return "quorumRemove";
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    super.run(cl);
    List<RaftPeerId> raftPeerIds = new ArrayList<>();
    for (String address : cl.getOptionValues(REMOVE_PEER_ADDRESS_OPTION_NAME)) {
      String[] str = address.split(":");
      if(str.length < 2) {
        println("The format of the parameter is wrong");
        return -1;
      }
      InetSocketAddress serverAddress = InetSocketAddress
              .createUnresolved(str[0], Integer.parseInt(str[1]));
      RaftPeerId peerId = RaftUtils.getPeerId(serverAddress);
      raftPeerIds.add(peerId);
    }

    try (RaftClient client = RaftUtils.createClient(getRaftGroup())) {
      List<RaftPeer> peers = new ArrayList<>();
      for (RaftPeer peer : getRaftGroup().getPeers()) {
        if (!raftPeerIds.contains(peer.getId())) {
          peers.add(RaftPeer.newBuilder(peer).build());
        }
      }
      RaftClientReply reply = client.admin().setConfiguration(peers);
      processReply(reply, () ->"failed to remove raft peer");
    }
    return 0;
  }

  @Override
  public String getUsage() {
    return String.format("%s"
                    + " -%s <PEER0_HOST:PEER0_PORT,PEER1_HOST:PEER1_PORT,PEER2_HOST:PEER2_PORT>"
                    + " [-%s <RAFT_GROUP_ID>]"
                    + " -%s <PEER_HOST:PEER_PORT>",
            getCommandName(), PEER_OPTION_NAME, GROUPID_OPTION_NAME, REMOVE_PEER_ADDRESS_OPTION_NAME);
  }

  @Override
  public String getDescription() {
    return description();
  }

  @Override
  public Options getOptions() {
    return super.getOptions().addOption(
            Option.builder()
                    .option(REMOVE_PEER_ADDRESS_OPTION_NAME)
                    .hasArg()
                    .required()
                    .desc("peer address to be removed")
                    .build());
  }

  /**
   * @return command's description
   */
  public static String description() {
    return "Remove peers of a ratis group";
  }
}
