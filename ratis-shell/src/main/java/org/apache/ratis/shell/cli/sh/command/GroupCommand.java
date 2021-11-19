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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Command for remove and add ratis server.
 */
public class GroupCommand extends AbstractRatisCommand {
  public static final String REMOVE_OPTION_NAME = "remove";
  public static final String ADD_OPTION_NAME = "add";

  /**
   * @param context command context
   */
  public GroupCommand(Context context) {
    super(context);
  }

  @Override
  public String getCommandName() {
    return "group";
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    super.run(cl);
    final Map<RaftPeerId, InetSocketAddress> peersInfo = new HashMap<>();
    final List<RaftPeerId> toRemove = getIds(cl.getOptionValues(REMOVE_OPTION_NAME), (a, b) -> {});
    final List<RaftPeerId> toAdd = getIds(cl.getOptionValues(ADD_OPTION_NAME), peersInfo::put);
    if (toRemove.isEmpty() && toAdd.isEmpty()) {
      throw new IllegalArgumentException(String.format("Both -%s and -%s options are empty",
          REMOVE_OPTION_NAME, ADD_OPTION_NAME));
    }

    try (RaftClient client = RaftUtils.createClient(getRaftGroup())) {
      final Stream<RaftPeer> remaining = getRaftGroup().getPeers().stream()
          .filter(raftPeer -> !toRemove.contains(raftPeer.getId()))
          .filter(raftPeer -> !toAdd.contains(raftPeer.getId()));
      final Stream<RaftPeer> adding = toAdd.stream().map(raftPeerId -> RaftPeer.newBuilder()
          .setId(raftPeerId)
          .setAddress(peersInfo.get(raftPeerId))
          .setPriority(0)
          .build());
      final List<RaftPeer> peers = Stream.concat(remaining, adding).collect(Collectors.toList());
      System.out.println("New peer list: " + peers);
      RaftClientReply reply = client.admin().setConfiguration(peers);
      processReply(reply, () -> "failed to change raft peer");
    }
    return 0;
  }

  private static List<RaftPeerId> getIds(String[] optionValues, BiConsumer<RaftPeerId, InetSocketAddress> consumer) {
    if (optionValues == null) {
      return Collections.emptyList();
    }
    final List<RaftPeerId> ids = new ArrayList<>();
    for (String address : optionValues) {
      final String[] str = parse(address);
      final InetSocketAddress serverAddress = InetSocketAddress.createUnresolved(str[0], Integer.parseInt(str[1]));
      final RaftPeerId peerId = RaftUtils.getPeerId(serverAddress);
      consumer.accept(peerId, serverAddress);
      ids.add(peerId);
    }
    return ids;
  }

  private static String[] parse(String address) {
    String[] str = address.split(":");
    if(str.length < 2) {
      throw new IllegalArgumentException("Failed to parse the address parameter \"" + address + "\".");
    }
    return str;
  }

  @Override
  public String getUsage() {
    return String.format("%s"
                    + " -%s <PEER0_HOST:PEER0_PORT,PEER1_HOST:PEER1_PORT,PEER2_HOST:PEER2_PORT>"
                    + " [-%s <RAFT_GROUP_ID>]"
                    + " -%s <PEER_HOST:PEER_PORT>"
                    + " -%s <PEER_HOST:PEER_PORT>",
            getCommandName(), PEER_OPTION_NAME, GROUPID_OPTION_NAME,
            REMOVE_OPTION_NAME, ADD_OPTION_NAME);
  }

  @Override
  public String getDescription() {
    return description();
  }

  @Override
  public Options getOptions() {
    return super.getOptions()
            .addOption(Option.builder()
                    .option(REMOVE_OPTION_NAME)
                    .hasArg()
                    .desc("peer address to be removed")
                    .build())
            .addOption(Option.builder()
                    .option(ADD_OPTION_NAME)
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
