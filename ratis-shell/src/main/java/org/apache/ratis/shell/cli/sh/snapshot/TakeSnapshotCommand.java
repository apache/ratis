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
package org.apache.ratis.shell.cli.sh.snapshot;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.shell.cli.RaftUtils;
import org.apache.ratis.shell.cli.sh.command.AbstractRatisCommand;
import org.apache.ratis.shell.cli.sh.command.Context;

import java.io.IOException;

/**
 * Command for make a ratis server take snapshot.
 */
public class TakeSnapshotCommand extends AbstractRatisCommand {
  public static final String TAKE_SNAPSHOT_TIMEOUT_OPTION_NAME = "snapshotTimeout";
  public static final String PEER_ID_OPTION_NAME = "peerId";

  /**
   * @param context command context
   */
  public TakeSnapshotCommand(Context context) {
    super(context);
  }

  @Override
  public String getCommandName() {
    return "create";
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    super.run(cl);
    long timeout;
    final RaftPeerId peerId;
    if (cl.hasOption(TAKE_SNAPSHOT_TIMEOUT_OPTION_NAME)) {
      timeout = Long.parseLong(cl.getOptionValue(TAKE_SNAPSHOT_TIMEOUT_OPTION_NAME));
    } else {
      timeout = 3000;
    }
    try(final RaftClient raftClient = RaftUtils.createClient(getRaftGroup())) {
      if (cl.hasOption(PEER_ID_OPTION_NAME)) {
        peerId = RaftPeerId.getRaftPeerId(cl.getOptionValue(PEER_ID_OPTION_NAME));
      } else {
        peerId = null;
      }
      RaftClientReply reply = raftClient.getSnapshotManagementApi(peerId).create(timeout);
      processReply(reply, () -> String.format("Failed to take snapshot of peerId %s", peerId));
      printf(String.format("Successful take snapshot on peerId %s, the latest snapshot index is %d",
          peerId, reply.getLogIndex()));
    }
    return 0;
  }

  @Override
  public String getUsage() {
    return String.format("%s"
            + " -%s <PEER0_HOST:PEER0_PORT,PEER1_HOST:PEER1_PORT,PEER2_HOST:PEER2_PORT>"
            + " [-%s <RAFT_GROUP_ID>]"
            + " [-%s <timeoutInMs>]"
            + " [-%s <raftPeerId>]",
        getCommandName(), PEER_OPTION_NAME, GROUPID_OPTION_NAME, TAKE_SNAPSHOT_TIMEOUT_OPTION_NAME);
  }

  @Override
  public String getDescription() {
    return description();
  }

  @Override
  public Options getOptions() {
    return super.getOptions()
        .addOption(Option.builder()
            .option(TAKE_SNAPSHOT_TIMEOUT_OPTION_NAME)
            .hasArg()
            .desc("timeout to wait taking snapshot in ms")
            .build())
        .addOption(Option.builder()
            .option(PEER_ID_OPTION_NAME)
            .hasArg()
            .desc("the id of server takeing snapshot")
            .build());
  }

  /**
   * @return command's description
   */
  public static String description() {
    return "Make a ratis server take snapshot";
  }
}
