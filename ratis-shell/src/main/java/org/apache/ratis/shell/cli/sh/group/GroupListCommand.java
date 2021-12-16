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
package org.apache.ratis.shell.cli.sh.group;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.GroupListReply;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.shell.cli.RaftUtils;
import org.apache.ratis.shell.cli.sh.command.AbstractRatisCommand;
import org.apache.ratis.shell.cli.sh.command.Context;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Command for querying the group information of a ratis server.
 */
public class GroupListCommand extends AbstractRatisCommand {
  public static final String SERVER_ADDRESS_OPTION_NAME = "serverAddress";
  public static final String PEER_ID_OPTION_NAME = "peerId";

  /**
   * @param context command context
   */
  public GroupListCommand(Context context) {
    super(context);
  }

  @Override
  public String getCommandName() {
    return "list";
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    super.run(cl);
    final RaftPeerId peerId;
    final String address;

    if (cl.hasOption(PEER_ID_OPTION_NAME)) {
      peerId = RaftPeerId.getRaftPeerId(cl.getOptionValue(PEER_ID_OPTION_NAME));
      address = getRaftGroup().getPeer(peerId).getAddress();
    } else if (cl.hasOption(SERVER_ADDRESS_OPTION_NAME)) {
      address = cl.getOptionValue(SERVER_ADDRESS_OPTION_NAME);
      final InetSocketAddress serverAddress = parseInetSocketAddress(address);
      peerId = RaftUtils.getPeerId(serverAddress);
    } else {
      throw new IllegalArgumentException(
              "Both " + PEER_ID_OPTION_NAME + " and " + SERVER_ADDRESS_OPTION_NAME
              + " options are missing.");
    }

    try(final RaftClient raftClient = RaftUtils.createClient(getRaftGroup())) {
      GroupListReply reply = raftClient.getGroupManagementApi(peerId).list();
      processReply(reply, () -> String.format("Failed to get group information of peerId %s (server %s)",
              peerId, address));
      printf(String.format("The peerId %s (server %s) is in %d groups, and the groupIds is: %s",
              peerId, address, reply.getGroupIds().size(), reply.getGroupIds()));
    }
    return 0;

  }

  @Override
  public String getUsage() {
    return String.format("%s"
                    + " -%s <PEER0_HOST:PEER0_PORT,PEER1_HOST:PEER1_PORT,PEER2_HOST:PEER2_PORT>"
                    + " [-%s <RAFT_GROUP_ID>]"
                    + " <[-%s <PEER0_HOST:PEER0_PORT>]|[-%s <peerId>]>",
            getCommandName(), PEER_OPTION_NAME, GROUPID_OPTION_NAME, SERVER_ADDRESS_OPTION_NAME,
            PEER_ID_OPTION_NAME);
  }

  @Override
  public String getDescription() {
    return description();
  }

  @Override
  public Options getOptions() {
    OptionGroup group = new OptionGroup();
    group.setRequired(true);
    group.addOption(new Option(null, SERVER_ADDRESS_OPTION_NAME, true, "the server address"));
    group.addOption(new Option(null, PEER_ID_OPTION_NAME, true, "the peer id"));
    return super.getOptions().addOptionGroup(group);
  }

  /**
   * @return command's description
   */
  public static String description() {
    return "Display the group information of a specific raft server";
  }
}
