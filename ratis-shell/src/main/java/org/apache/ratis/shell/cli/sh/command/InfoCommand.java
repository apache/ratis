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

import org.apache.ratis.shell.cli.RaftUtils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.GroupInfoReply;

import java.io.IOException;

/**
 * Command for querying ratis group information.
 */
public class InfoCommand extends AbstractRatisCommand {

  /**
   * @param context command context
   */
  public InfoCommand(Context context) {
    super(context);
  }

  @Override
  public String getCommandName() {
    return "info";
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    super.run(cl);
    getPrintStream().println("group id: " + getRaftGroup().getGroupId().getUuid());
    try (RaftClient client = RaftUtils.createClient(getRaftGroup())) {
      GroupInfoReply reply =
          client.getGroupManagementApi(
              getRaftGroup().getPeers().stream()
                  .findFirst()
                  .get()
                  .getId())
              .info(getRaftGroup().getGroupId());
      processReply(reply,
          "failed to get info");
      RaftProtos.RaftPeerProto leader =
          getLeader(reply.getRoleInfoProto());
      getPrintStream().printf("leader info: %s(%s)%n%n",
          leader.getId().toStringUtf8(), leader.getAddress());
      getPrintStream().println(reply.getCommitInfos());
    }
    return 0;
  }

  @Override
  public String getUsage() {
    return String.format("%s"
        + " [-%s PEER0_HOST:PEER0_PORT,PEER1_HOST:PEER1_PORT,PEER2_HOST:PEER2_PORT]"
        + " [-%s RAFT_GROUP_ID]",
        getCommandName(), PEER_OPTION_NAME, GROUPID_OPTION_NAME);
  }

  @Override
  public String getDescription() {
    return description();
  }

  @Override
  public Options getOptions() {
    return super.getOptions();
  }

  /**
   * @return command's description
   */
  public static String description() {
    return "Display the information of a specific raft group";
  }
}
