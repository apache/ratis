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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Command for remove ratis server.
 */
public class QuorumCommand extends AbstractRatisCommand {
  public static final String REMOVE_PEER_ADDRESS_OPTION_NAME = "removePeer";
  public static final String ADD_PEER_ADDRESS_OPTION_NAME = "addPeer";

  /**
   * @param context command context
   */
  public QuorumCommand(Context context) {
    super(context);
  }

  @Override
  public String getCommandName() {
    return "group";
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    super.run(cl);
    List<RaftPeerId> removePeerIds = new ArrayList<>();
    List<RaftPeerId> addPeerIds = new ArrayList<>();
    Map<RaftPeerId, InetSocketAddress> peersInfo = new HashMap<>();
    if(getIds(cl, removePeerIds, REMOVE_PEER_ADDRESS_OPTION_NAME, new HashMap<>())
      & getIds(cl, addPeerIds, ADD_PEER_ADDRESS_OPTION_NAME, peersInfo)) {
      return -1;
    }

    try (RaftClient client = RaftUtils.createClient(getRaftGroup())) {
      List<RaftPeer> peers =
              getRaftGroup().getPeers().stream()
              .filter(raftPeer -> !removePeerIds.contains(raftPeer.getId()))
              .filter(raftPeer -> !addPeerIds.contains(raftPeer.getId()))
              .collect(Collectors.toList());
      System.out.println(peers);
      peers.addAll(addPeerIds.stream()
                      .map(raftPeerId -> RaftPeer.newBuilder().setId(raftPeerId)
                      .setAddress(peersInfo.get(raftPeerId))
                      .setPriority(0)
                      .build()).collect(Collectors.toList()));
      System.out.println(peers);
      RaftClientReply reply = client.admin().setConfiguration(peers);
      processReply(reply, () ->"failed to change raft peer");
    }
    return 0;
  }

  private boolean getIds(CommandLine cl, List<RaftPeerId> ids, String addressOptionName,
                         Map<RaftPeerId, InetSocketAddress> infos) {
    if (cl.getOptionValues(addressOptionName) == null) {
      return false;
    }
    for (String address : cl.getOptionValues(addressOptionName)) {
      String[] str = isFormat(address);
      if (str == null) {
        return true;
      }
      ids.add(addressToPeerId(str, infos));
    }
    return false;
  }

  private String[] isFormat(String address) {
    String[] str = address.split(":");
    if(str.length < 2) {
      println("The format of the parameter is wrong");
      return null;
    }
    return str;
  }

  private RaftPeerId addressToPeerId(String[] str, Map<RaftPeerId, InetSocketAddress> infos) {
    InetSocketAddress serverAddress = InetSocketAddress
            .createUnresolved(str[0], Integer.parseInt(str[1]));
    RaftPeerId peerId = RaftUtils.getPeerId(serverAddress);
    infos.put(peerId, serverAddress);
    return peerId;
  }

  @Override
  public String getUsage() {
    return String.format("%s"
                    + " -%s <PEER0_HOST:PEER0_PORT,PEER1_HOST:PEER1_PORT,PEER2_HOST:PEER2_PORT>"
                    + " [-%s <RAFT_GROUP_ID>]"
                    + " -%s <PEER_HOST:PEER_PORT>"
                    + " -%s <PEER_HOST:PEER_PORT>",
            getCommandName(), PEER_OPTION_NAME, GROUPID_OPTION_NAME,
            REMOVE_PEER_ADDRESS_OPTION_NAME, ADD_PEER_ADDRESS_OPTION_NAME);
  }

  @Override
  public String getDescription() {
    return description();
  }

  @Override
  public Options getOptions() {
    return super.getOptions()
            .addOption(Option.builder()
                    .option(REMOVE_PEER_ADDRESS_OPTION_NAME)
                    .hasArg()
                    .desc("peer address to be removed")
                    .build())
            .addOption(Option.builder()
                    .option(ADD_PEER_ADDRESS_OPTION_NAME)
                    .hasArg()
                    .desc("peer address to be added")
                    .build());
  }

  /**
   * @return command's description
   */
  public static String description() {
    return "Remove or Add peers of a ratis group";
  }
}
