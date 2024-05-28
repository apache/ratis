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
package org.apache.ratis.shell.cli.sh.peer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.shell.cli.RaftUtils;
import org.apache.ratis.shell.cli.sh.command.AbstractRatisCommand;
import org.apache.ratis.shell.cli.sh.command.Context;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Command for add ratis server.
 */
public class AddCommand extends AbstractRatisCommand {

  public static final String ADDRESS_OPTION_NAME = "address";
  public static final String PEER_ID_OPTION_NAME = "peerId";
  /**
   * @param context command context
   */
  public AddCommand(Context context) {
    super(context);
  }

  @Override
  public String getCommandName() {
    return "add";
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    super.run(cl);
    final Map<RaftPeerId, InetSocketAddress> peersInfo = new HashMap<>();
    List<RaftPeerId> ids;

    if (cl.hasOption(ADDRESS_OPTION_NAME) && cl.hasOption(PEER_ID_OPTION_NAME)) {
      ids = Arrays.stream(cl.getOptionValue(PEER_ID_OPTION_NAME).split(","))
          .map(RaftPeerId::getRaftPeerId).collect(Collectors.toList());
      List<InetSocketAddress> addresses =
          Arrays.stream(cl.getOptionValue(ADDRESS_OPTION_NAME).split(","))
              .map(s -> parseInetSocketAddress(s)).collect(Collectors.toList());
      assert ids.size() == addresses.size();
      for (int i = 0; i < ids.size(); i++) {
        peersInfo.put(ids.get(i), addresses.get(i));
      }
    } else if (cl.hasOption(ADDRESS_OPTION_NAME)) {
      ids = getIds(cl.getOptionValue(ADDRESS_OPTION_NAME).split(","), peersInfo::put);
    } else {
      throw new IllegalArgumentException(
          "Both " + PEER_ID_OPTION_NAME + " and " + ADDRESS_OPTION_NAME + " options are missing.");
    }

    try (RaftClient client = RaftUtils.createClient(getRaftGroup())) {
      final Stream<RaftPeer> remaining = getPeerStream(RaftPeerRole.FOLLOWER);
      final Stream<RaftPeer> adding = ids.stream().map(raftPeerId -> RaftPeer.newBuilder()
          .setId(raftPeerId)
          .setAddress(peersInfo.get(raftPeerId))
          .setPriority(0)
          .build());
      final List<RaftPeer> peers = Stream.concat(remaining, adding).collect(Collectors.toList());
      final List<RaftPeer> listeners = getPeerStream(RaftPeerRole.LISTENER)
          .collect(Collectors.toList());
      System.out.println("New peer list: " + peers);
      System.out.println("New listener list:  " + listeners);
      RaftClientReply reply = client.admin().setConfiguration(peers, listeners);
      processReply(reply, () -> "Failed to change raft peer");
    }
    return 0;
  }

  @Override
  public String getUsage() {
    return String.format("%s"
            + " -%s <PEER0_HOST:PEER0_PORT,PEER1_HOST:PEER1_PORT,PEER2_HOST:PEER2_PORT>"
            + " [-%s <RAFT_GROUP_ID>]"
            + " <[-%s <PEER0_HOST:PEER0_PORT>]|[-%s <peerId>]>",
        getCommandName(), PEER_OPTION_NAME, GROUPID_OPTION_NAME,
        ADDRESS_OPTION_NAME, PEER_ID_OPTION_NAME);
  }

  @Override
  public String getDescription() {
    return description();
  }

  @Override
  public Options getOptions() {
    return super.getOptions()
        .addOption(Option.builder()
            .option(ADDRESS_OPTION_NAME)
            .hasArg()
            .desc("The address information of ratis peers")
            .build())
        .addOption(Option.builder()
            .option(PEER_ID_OPTION_NAME).hasArg()
            .desc("The peer id of ratis peers")
            .build());
  }

  /**
   * @return command's description
   */
  public static String description() {
    return "Add peers to a ratis group";
  }
}
