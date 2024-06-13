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

import org.apache.commons.cli.Option;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.GroupInfoReply;
import org.apache.ratis.shell.cli.RaftUtils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.proto.RaftProtos.RaftConfigurationProto;
import org.apache.ratis.proto.RaftProtos.FollowerInfoProto;
import org.apache.ratis.proto.RaftProtos.RaftPeerProto;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.proto.RaftProtos.RoleInfoProto;
import org.apache.ratis.util.ProtoUtils;

import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.ratis.shell.cli.RaftUtils.buildRaftGroupIdFromStr;
import static org.apache.ratis.shell.cli.RaftUtils.buildRaftPeersFromStr;
import static org.apache.ratis.shell.cli.RaftUtils.retrieveGroupInfoByGroupId;
import static org.apache.ratis.shell.cli.RaftUtils.retrieveRemoteGroupId;

/**
 * The base class for the ratis shell which need to connect to server.
 */
public abstract class AbstractRatisCommand extends AbstractCommand {
  public static final String PEER_OPTION_NAME = "peers";
  public static final String GROUPID_OPTION_NAME = "groupid";
  private RaftGroup raftGroup;
  private GroupInfoReply groupInfoReply;

  protected AbstractRatisCommand(Context context) {
    super(context);
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    List<RaftPeer> peers = buildRaftPeersFromStr(cl.getOptionValue(PEER_OPTION_NAME));
    RaftGroupId raftGroupIdFromConfig = buildRaftGroupIdFromStr(cl.getOptionValue(GROUPID_OPTION_NAME));
    raftGroup = RaftGroup.valueOf(raftGroupIdFromConfig, peers);
    PrintStream printStream = getPrintStream();
    try (final RaftClient client = RaftUtils.createClient(raftGroup)) {
      final RaftGroupId remoteGroupId = retrieveRemoteGroupId(raftGroupIdFromConfig, peers, client, printStream);
      groupInfoReply = retrieveGroupInfoByGroupId(remoteGroupId, peers, client, printStream);
      raftGroup = groupInfoReply.getGroup();
    }
    return 0;
  }

  @Override
  public Options getOptions() {
    return new Options()
            .addOption(
                Option.builder()
                    .option(PEER_OPTION_NAME)
                    .hasArg()
                    .required()
                    .desc("Peer addresses seperated by comma")
                    .build())
            .addOption(GROUPID_OPTION_NAME, true, "Raft group id");
  }

  protected RaftGroup getRaftGroup() {
    return raftGroup;
  }

  protected GroupInfoReply getGroupInfoReply() {
    return groupInfoReply;
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

  protected void processReply(RaftClientReply reply, Supplier<String> messageSupplier) throws IOException {
    RaftUtils.processReply(reply, getPrintStream()::println, messageSupplier);
  }

  protected List<RaftPeerId> getIds(String[] optionValues, BiConsumer<RaftPeerId, InetSocketAddress> consumer) {
    if (optionValues == null) {
      return Collections.emptyList();
    }
    final List<RaftPeerId> ids = new ArrayList<>();
    for (String address : optionValues) {
      final InetSocketAddress serverAddress = parseInetSocketAddress(address);
      final RaftPeerId peerId = RaftUtils.getPeerId(serverAddress);
      consumer.accept(peerId, serverAddress);
      ids.add(peerId);
    }
    return ids;
  }

  protected Stream<RaftPeer> getPeerStream(RaftPeerRole role) {
    final RaftConfigurationProto conf = groupInfoReply.getConf().orElse(null);
    if (conf == null) {
      // Assume all peers are followers in order preserve the pre-listener behaviors.
      return role == RaftPeerRole.FOLLOWER ? getRaftGroup().getPeers().stream() : Stream.empty();
    }
    final Set<RaftPeer> targets = (role == RaftPeerRole.LISTENER ? conf.getListenersList() : conf.getPeersList())
        .stream()
        .map(ProtoUtils::toRaftPeer)
        .collect(Collectors.toSet());
    return getRaftGroup()
        .getPeers()
        .stream()
        .filter(targets::contains);
  }
}
