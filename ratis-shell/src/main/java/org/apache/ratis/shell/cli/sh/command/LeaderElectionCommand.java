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
import java.util.Optional;

public class LeaderElectionCommand extends AbstractRatisCommand{
  public static final String ADDRESS_OPTION_NAME = "address";
  public static final String PAUSE_OPTION_NAME = "pause";
  public static final String RESUME_OPTION_NAME = "resume";

  /**
   * @param context command context
   */
  public LeaderElectionCommand(Context context) {
    super(context);
  }

  @Override
  public String getCommandName() {
    return "leaderElection";
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    super.run(cl);

    String strAddr = cl.getOptionValue(ADDRESS_OPTION_NAME);
    final RaftPeerId peerId;
    Optional<RaftPeer> peer =
        getRaftGroup().getPeers().stream().filter(p -> p.getAddress().equals(strAddr)).findAny();
    if (peer.isPresent()) {
      peerId = peer.get().getId();
    } else {
      printf("Can't find a sever with the address:%s", strAddr);
      return -1;
    }
    try(final RaftClient raftClient = RaftUtils.createClient(getRaftGroup())) {
      RaftClientReply reply;
      if (cl.hasOption(PAUSE_OPTION_NAME) && cl.hasOption(RESUME_OPTION_NAME)) {
        printf("Can't pause and resume leader election to a sever at the same time");
        return -1;
      } else if (cl.hasOption(PAUSE_OPTION_NAME)) {
        reply = raftClient.getLeaderElectionManagementApi(peerId).pause();
        processReply(reply, () -> String.format("Failed to pause leader election of peer %s", strAddr));
      } else if (cl.hasOption(RESUME_OPTION_NAME)) {
        reply = raftClient.getLeaderElectionManagementApi(peerId).resume();
        processReply(reply, () -> String.format("Failed to resume leader election of peer %s", strAddr));
      } else {
        printf("Failed to execute command, missing option %s or s%", PAUSE_OPTION_NAME, RESUME_OPTION_NAME);
        return -1;
      }
      printf(String.format("Successful execute command on peer %s", strAddr));
    }
    return 0;
  }

  @Override
  public String getUsage() {
    return String.format("%s -%s <HOSTNAME:PORT>"
            + " -%s <PEER0_HOST:PEER0_PORT,PEER1_HOST:PEER1_PORT,PEER2_HOST:PEER2_PORT>"
            + " [-%s <RAFT_GROUP_ID>]"
            + " [-%s, -%s]",
        getCommandName(), ADDRESS_OPTION_NAME, PEER_OPTION_NAME,
        GROUPID_OPTION_NAME, PAUSE_OPTION_NAME, RESUME_OPTION_NAME);
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
            .required()
            .desc("Server address that will be paused or resume its leader election")
            .build())
        .addOption(Option.builder()
            .option(PAUSE_OPTION_NAME)
            .desc("Pause leader election on the specific server")
            .build())
        .addOption(Option.builder()
            .option(RESUME_OPTION_NAME)
            .desc("Resume leader election on the specific server")
            .build());
  }

  /**
   * @return command's description
   */
  public static String description() {
    return "Pause or resume leader election to the server <hostname>:<port>";
  }
}
