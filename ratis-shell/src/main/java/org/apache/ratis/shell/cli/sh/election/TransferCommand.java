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
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Command for transferring the ratis leader to specific server.
 */
public class TransferCommand extends AbstractRatisCommand {
  public static final String ADDRESS_OPTION_NAME = "address";
  /**
   * @param context command context
   */
  public TransferCommand(Context context) {
    super(context);
  }

  @Override
  public String getCommandName() {
    return "transfer";
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    super.run(cl);

    String strAddr = cl.getOptionValue(ADDRESS_OPTION_NAME);

    RaftPeerId newLeaderId = null;
    // update priorities to enable transfer
    List<RaftPeer> peersWithNewPriorities = new ArrayList<>();
    for (RaftPeer peer : getRaftGroup().getPeers()) {
      peersWithNewPriorities.add(
          RaftPeer.newBuilder(peer)
              .setPriority(peer.getAddress().equals(strAddr) ? 2 : 1)
              .build()
      );
      if (peer.getAddress().equals(strAddr)) {
        newLeaderId = peer.getId();
      }
    }
    if (newLeaderId == null) {
      return -2;
    }
    try (RaftClient client = RaftUtils.createClient(getRaftGroup())) {
      String stringPeers = "[" + peersWithNewPriorities.stream().map(RaftPeer::toString)
          .collect(Collectors.joining(", ")) + "]";
      printf("Applying new peer state before transferring leadership: %n%s%n", stringPeers);
      RaftClientReply setConfigurationReply =
          client.admin().setConfiguration(peersWithNewPriorities);
      processReply(setConfigurationReply,
          () -> "failed to set priorities before initiating election");
      // transfer leadership
      printf("Transferring leadership to server with address <%s> %n", strAddr);
      try {
        Thread.sleep(3_000);
        RaftClientReply transferLeadershipReply =
            client.admin().transferLeadership(newLeaderId, 60_000);
        processReply(transferLeadershipReply, () -> "election failed");
      } catch (Throwable t) {
        printf("caught an error when executing transfer: %s%n", t.getMessage());
        return -1;
      }
      println("Transferring leadership initiated");
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
            .desc("Server address that will take over as leader")
            .build()
    );
  }

  /**
   * @return command's description
   */
  public static String description() {
    return "Transfers leadership to the <hostname>:<port>";
  }
}
