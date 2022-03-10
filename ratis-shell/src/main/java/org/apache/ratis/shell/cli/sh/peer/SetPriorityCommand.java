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
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.shell.cli.RaftUtils;
import org.apache.ratis.shell.cli.sh.command.AbstractRatisCommand;
import org.apache.ratis.shell.cli.sh.command.Context;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SetPriorityCommand extends AbstractRatisCommand {

  public static final String PEER_WITH_NEW_PRIORITY_OPTION_NAME = "addressPriority";

  /**
   * @param context command context
   */
  public SetPriorityCommand(Context context) {
    super(context);
  }

  @Override
  public String getCommandName() {
    return "setPriority";
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    super.run(cl);
    Map<String, Integer> addressPriorityMap = new HashMap<>();
    for (String optionValue : cl.getOptionValues(PEER_WITH_NEW_PRIORITY_OPTION_NAME)) {
      String[] str = optionValue.split("[|]");
      if(str.length < 2) {
        println("The format of the parameter is wrong");
        return -1;
      }
      addressPriorityMap.put(str[0], Integer.parseInt(str[1]));
    }

    try (RaftClient client = RaftUtils.createClient(getRaftGroup())) {
      List<RaftPeer> peers = new ArrayList<>();
      for (RaftPeer peer : getRaftGroup().getPeers()) {
        if (!addressPriorityMap.containsKey(peer.getAddress())) {
          peers.add(RaftPeer.newBuilder(peer).build());
        } else {
          peers.add(RaftPeer.newBuilder(peer)
              .setPriority(addressPriorityMap.get(peer.getAddress()))
              .build()
          );
        }
      }
      RaftClientReply reply = client.admin().setConfiguration(peers);
      processReply(reply, () -> "Failed to set master priorities ");
    }
    return 0;
  }

  @Override
  public String getUsage() {
    return String.format("%s"
            + " -%s <PEER0_HOST:PEER0_PORT,PEER1_HOST:PEER1_PORT,PEER2_HOST:PEER2_PORT>"
            + " [-%s <RAFT_GROUP_ID>]"
            + " -%s <PEER_HOST:PEER_PORT|PRIORITY>",
        getCommandName(), PEER_OPTION_NAME, GROUPID_OPTION_NAME, PEER_WITH_NEW_PRIORITY_OPTION_NAME);
  }

  @Override
  public String getDescription() {
    return description();
  }

  @Override
  public Options getOptions() {
    return super.getOptions().addOption(
        Option.builder()
            .option(PEER_WITH_NEW_PRIORITY_OPTION_NAME)
            .hasArg()
            .required()
            .desc("Peers information with priority")
            .build());
  }

  /**
   * @return command's description
   */
  public static String description() {
    return "Set priority to ratis peers";
  }
}
