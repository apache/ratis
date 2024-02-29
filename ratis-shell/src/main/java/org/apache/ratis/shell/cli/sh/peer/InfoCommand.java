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
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.PeerInfoReply;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.shell.cli.RaftUtils;
import org.apache.ratis.shell.cli.sh.command.AbstractRatisCommand;
import org.apache.ratis.shell.cli.sh.command.Context;

import java.io.IOException;


public class InfoCommand extends AbstractRatisCommand{
  public static final String PEER_ID_OPTION_NAME = "peerId";

  public InfoCommand(Context context) {
    super(context);
  }

  @Override
  public String getCommandName() {
    return "info";
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    final RaftPeerId peerId;
    if (cl.hasOption(PEER_ID_OPTION_NAME)) {
      peerId = RaftPeerId.getRaftPeerId(cl.getOptionValue(PEER_ID_OPTION_NAME));
    } else {
      throw new IllegalArgumentException(
          PEER_ID_OPTION_NAME + " options are missing.");
    }
    super.run(cl);

    try(final RaftClient raftClient = RaftUtils.createClient(getRaftGroup())) {
      PeerInfoReply peerInfoReply = raftClient.getPeerManagementApi(peerId).info();

      printf(String.format("For peerId %s, current term: %d, last commitIndex: %d, last appliedIndex: %d"
              + ", last snapshotIndex: %d",
          peerInfoReply.getServerId(), peerInfoReply.getCurrentTerm(), peerInfoReply.getLastCommitIndex(),
          peerInfoReply.getLastAppliedIndex(), peerInfoReply.getLastSnapshotIndex()));
    }

    return 0;
  }

  @Override
  public String getUsage() {
    return String.format("%s"
            + " -%s <PEER0_HOST:PEER0_PORT,PEER1_HOST:PEER1_PORT,PEER2_HOST:PEER2_PORT>"
            + " [-%s <RAFT_GROUP_ID>]"
            + " -%s <peerId>",
        getCommandName(), PEER_OPTION_NAME, GROUPID_OPTION_NAME, PEER_ID_OPTION_NAME);
  }



  @Override
  public String getDescription() {
    return description();
  }

  @Override
  public Options getOptions() {
    OptionGroup group = new OptionGroup();
    group.setRequired(true);
    group.addOption(new Option(null, PEER_ID_OPTION_NAME, true, "the peer id"));
    return super.getOptions().addOptionGroup(group);
  }


  public static String description() {
    return "Display information of of current term," +
        " last commit index, last applied index," +
        " last snapshot index of the peer.";
  }


}
