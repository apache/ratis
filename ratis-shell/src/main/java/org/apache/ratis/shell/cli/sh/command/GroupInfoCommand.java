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
import org.apache.ratis.protocol.GroupListReply;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.shell.cli.RaftUtils;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Command for querying the group information of a ratis server.
 */
public class GroupInfoCommand extends AbstractRatisCommand{
  public static final String SERVER_ADDRESS_OPTION_NAME = "serverAddress";
  /**
   * @param context command context
   */
  public GroupInfoCommand(Context context) {
    super(context);
  }

  @Override
  public String getCommandName() {
    return "groupInfo";
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    super.run(cl);
    String strAddr = cl.getOptionValue(SERVER_ADDRESS_OPTION_NAME);

    String[] str = strAddr.split(":");
    if(str.length < 2) {
      throw new IllegalArgumentException("Failed to parse the server address parameter \"" + strAddr + "\".");
    }
    final InetSocketAddress serverAddress = InetSocketAddress.createUnresolved(str[0], Integer.parseInt(str[1]));
    final RaftPeerId peerId = RaftUtils.getPeerId(serverAddress);

    try(final RaftClient raftClient = RaftUtils.createClient(getRaftGroup())){
      GroupListReply reply = raftClient.getGroupManagementApi(peerId).list();
      processReply(reply, () -> String.format("Failed to get group information of server %s",strAddr));
      printf(String.format("The server %s is in %d groups, and the groupIds is: %s",
              strAddr, reply.getGroupIds().size(),reply.getGroupIds()));
    }
    return 0;
  }

  @Override
  public String getUsage() {
    return String.format("%s"
                    + " -%s <PEER0_HOST:PEER0_PORT,PEER1_HOST:PEER1_PORT,PEER2_HOST:PEER2_PORT>"
                    + "-%s <PEER0_HOST:PEER0_PORT>",
            getCommandName(), PEER_OPTION_NAME, SERVER_ADDRESS_OPTION_NAME);
  }

  @Override
  public String getDescription() {
    return description();
  }

  @Override
  public Options getOptions() {
    return super.getOptions().addOption(
        Option.builder()
            .option(SERVER_ADDRESS_OPTION_NAME)
            .hasArg()
            .required()
            .desc("the server address")
            .build());
  }

  /**
   * @return command's description
   */
  public static String description() {
    return "Display the group information of a specific raft server";
  }
}
