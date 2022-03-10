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
package org.apache.ratis.shell.cli.sh.election;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.shell.cli.RaftUtils;
import org.apache.ratis.shell.cli.sh.command.AbstractRatisCommand;
import org.apache.ratis.shell.cli.sh.command.Context;

import java.io.IOException;

/**
 * Command for resuming leader election on specific server
 */
public class ResumeCommand extends AbstractRatisCommand {

  public static final String ADDRESS_OPTION_NAME = "address";
  /**
   * @param context command context
   */
  public ResumeCommand(Context context) {
    super(context);
  }

  @Override
  public String getCommandName() {
    return "resume";
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    super.run(cl);

    String strAddr = cl.getOptionValue(ADDRESS_OPTION_NAME);
    final RaftPeerId peerId = getRaftGroup().getPeers().stream()
        .filter(p -> p.getAddress().equals(strAddr)).findAny()
        .map(RaftPeer::getId)
        .orElse(null);
    if (peerId == null) {
      printf("Can't find a sever with the address:%s", strAddr);
      return -1;
    }
    try(final RaftClient raftClient = RaftUtils.createClient(getRaftGroup())) {
      RaftClientReply reply = raftClient.getLeaderElectionManagementApi(peerId).resume();
      processReply(reply, () -> String.format("Failed to resume leader election on peer %s", strAddr));
      printf(String.format("Successful pause leader election on peer %s", strAddr));
    }
    return 0;
  }

  @Override
  public String getUsage() {
    return String.format("%s -%s <HOSTNAME:PORT>"
            + " -%s <PEER0_HOST:PEER0_PORT,PEER1_HOST:PEER1_PORT,PEER2_HOST:PEER2_PORT>"
            + " [-%s <RAFT_GROUP_ID>]",
        getCommandName(), ADDRESS_OPTION_NAME, PEER_OPTION_NAME,
        GROUPID_OPTION_NAME);
  }

  @Override
  public String getDescription() {
    return description();
  }

  @Override
  public Options getOptions() {
    return super.getOptions().addOption(
        Option.builder()
            .option(ADDRESS_OPTION_NAME)
            .hasArg()
            .required()
            .desc("Server address that will be resumed its leader election")
            .build()
    );
  }

  /**
   * @return command's description
   */
  public static String description() {
    return "Resume leader election to the server <hostname>:<port>";
  }
}
