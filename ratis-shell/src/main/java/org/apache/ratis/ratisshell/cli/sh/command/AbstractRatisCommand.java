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
package org.apache.ratis.ratisshell.cli.sh.command;

import org.apache.ratis.ratisshell.RetryUtil;
import org.apache.ratis.ratisshell.cli.Command;
import org.apache.ratis.ratisshell.cli.RaftUtils;
import org.apache.ratis.ratisshell.conf.InstancedConfiguration;
import org.apache.ratis.ratisshell.conf.PropertyKey;
import org.apache.ratis.ratisshell.conf.RatisShellConfiguration;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.proto.RaftProtos.FollowerInfoProto;
import org.apache.ratis.proto.RaftProtos.RaftPeerProto;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.proto.RaftProtos.RoleInfoProto;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;

import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * The base class for all the ratis shell {@link Command} classes.
 */
public abstract class AbstractRatisCommand implements Command {
  public static final String SERVICE_ID_OPTION_NAME = "serviceid";
  public static final String PEER_OPTION_NAME = "peers";
  public static final String GROUPID_OPTION_NAME = "groupid";
  public static final RaftGroupId DEFAULT_RAFT_GROUP_ID
      = RaftGroupId.valueOf(
      UUID.fromString("1-1-1-1-1"));
  private final PrintStream printStream;
  private RaftGroup raftGroup;

  protected AbstractRatisCommand(Context context) {
    printStream = context.getPrintStream();
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    RatisShellConfiguration conf = InstancedConfiguration.defaults();
    List<InetSocketAddress> addresses = new ArrayList<>();
    String peersStr = "";
    if (cl.hasOption(PEER_OPTION_NAME)) {
      peersStr = cl.getOptionValue(PEER_OPTION_NAME);
    } else {
      if (cl.hasOption(SERVICE_ID_OPTION_NAME)) {
        peersStr = conf.get(
            PropertyKey.Template.RATIS_SHELL_PEER_IDS.format(
                cl.getOptionValue(SERVICE_ID_OPTION_NAME)));
      }
    }
    String[] peersArray = peersStr.split(",");
    for (int i = 0; i < peersArray.length; i++) {
      String[] hostPortPair = peersArray[i].split(":");
      InetSocketAddress addr =
          new InetSocketAddress(hostPortPair[0], Integer.parseInt(hostPortPair[1]));
      addresses.add(addr);
    }

    RaftGroupId raftGroupIdFromConfig = DEFAULT_RAFT_GROUP_ID;
    if (cl.hasOption(GROUPID_OPTION_NAME)) {
      raftGroupIdFromConfig = RaftGroupId.valueOf(
          UUID.fromString(cl.getOptionValue(GROUPID_OPTION_NAME)));
    } else {
      if (cl.hasOption(SERVICE_ID_OPTION_NAME)) {
        PropertyKey groupIdKey =
            PropertyKey.Template.RATIS_SHELL_GROUP_ID.format(
                cl.getOptionValue(SERVICE_ID_OPTION_NAME));
        try {
          raftGroupIdFromConfig =
              RaftGroupId.valueOf(UUID.fromString(conf.get(groupIdKey)));
        } catch (IllegalArgumentException e) {
          // do nothing
        }
      }
    }

    List<RaftPeer> peers = addresses.stream()
        .map(addr -> RaftPeer.newBuilder()
            .setId(RaftUtils.getPeerId(addr))
            .setAddress(addr)
            .build()
        ).collect(Collectors.toList());
    raftGroup = RaftGroup.valueOf(raftGroupIdFromConfig, peers);
    try (final RaftClient client = RaftUtils.createClient(raftGroup)) {
      RaftGroupId remoteGroupId;
      List<RaftGroupId> groupIds;
      groupIds = RetryUtil.run(peers,
          p -> {
            try {
              return client.getGroupManagementApi((p.getId())).list().getGroupIds();
            } catch (IOException e) {
              e.printStackTrace();
              return null;
            }
          }
      );

      if (groupIds.size() == 1) {
        remoteGroupId = groupIds.get(0);
      } else {
        final UUID raftGroupUuid = raftGroupIdFromConfig.getUuid();
        Optional<RaftGroupId> raftGroupId =
            groupIds.stream().filter(r -> raftGroupUuid.equals(r.getUuid()))
                .findFirst();
        if (!raftGroupId.isPresent()) {
          printStream.println(
              "there are more than one group, you should specific one."
                  + groupIds);
          return -1;
        } else {
          remoteGroupId = raftGroupId.get();
        }
      }
      // TODO(maobaolong): failover to other peer if communicate failure
      raftGroup = RetryUtil.run(peers,
          p -> {
            try {
              return client.getGroupManagementApi((p.getId()))
                  .info(remoteGroupId).getGroup();
            } catch (IOException e) {
              e.printStackTrace();
              return null;
            }
          }
      );
    }
    return 0;
  }

  @Override
  public void validateArgs(CommandLine cl) throws IllegalArgumentException {
    if (!cl.hasOption(SERVICE_ID_OPTION_NAME)
        && !cl.hasOption(PEER_OPTION_NAME)) {
      throw new IllegalArgumentException(String.format(
          "should provide at least one of [%s] and [%s]",
          SERVICE_ID_OPTION_NAME, PEER_OPTION_NAME));
    }
  }

  @Override
  public Options getOptions() {
    return new Options()
            .addOption(PEER_OPTION_NAME, true, "Peer addresses seperated by comma")
            .addOption(GROUPID_OPTION_NAME, true, "Raft group id")
            .addOption(SERVICE_ID_OPTION_NAME, true, "Service id");
  }

  protected PrintStream getPrintStream() {
    return printStream;
  }

  protected RaftGroup getRaftGroup() {
    return raftGroup;
  }

  /**
   * Get the leader id.
   *
   * @param roleInfo the role info
   * @return the leader id
   */
  protected RaftPeerProto getLeader(RoleInfoProto roleInfo) {
    if (roleInfo == null) {
      return null;
    }
    if (roleInfo.getRole() == RaftPeerRole.LEADER) {
      return roleInfo.getSelf();
    }
    FollowerInfoProto followerInfo = roleInfo.getFollowerInfo();
    if (followerInfo == null) {
      return null;
    }
    return followerInfo.getLeaderInfo().getId();
  }

  protected void processReply(RaftClientReply reply, String msg)
      throws IOException {
    RaftUtils.processReply(reply, msg, printStream);
  }
}
