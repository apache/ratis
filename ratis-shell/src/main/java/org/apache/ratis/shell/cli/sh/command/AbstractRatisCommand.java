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
import org.apache.ratis.protocol.*;
import org.apache.ratis.protocol.exceptions.RaftException;
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
import org.apache.ratis.util.function.CheckedFunction;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The base class for the ratis shell which need to connect to server.
 */
public abstract class AbstractRatisCommand extends AbstractCommand {
  public static final String PEER_OPTION_NAME = "peers";
  public static final String GROUPID_OPTION_NAME = "groupid";
  public static final RaftGroupId DEFAULT_RAFT_GROUP_ID = RaftGroupId.randomId();

  /**
   * Execute a given function with input parameter from the members of a list.
   *
   * @param list the input parameters
   * @param function the function to be executed
   * @param <T> parameter type
   * @param <K> return value type
   * @param <E> the exception type thrown by the given function.
   * @return the value returned by the given function.
   */
  public static <T, K, E extends Throwable> K run(Collection<T> list, CheckedFunction<T, K, E> function) {
    for (T t : list) {
      try {
        K ret = function.apply(t);
        if (ret != null) {
          return ret;
        }
      } catch (Throwable e) {
        e.printStackTrace();
      }
    }
    return null;
  }

  private RaftGroup raftGroup;
  private GroupInfoReply groupInfoReply;

  protected AbstractRatisCommand(Context context) {
    super(context);
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    List<InetSocketAddress> addresses = new ArrayList<>();
    String peersStr = cl.getOptionValue(PEER_OPTION_NAME);
    String[] peersArray = peersStr.split(",");
    for (String peer : peersArray) {
      addresses.add(parseInetSocketAddress(peer));
    }

    final RaftGroupId raftGroupIdFromConfig = cl.hasOption(GROUPID_OPTION_NAME)?
        RaftGroupId.valueOf(UUID.fromString(cl.getOptionValue(GROUPID_OPTION_NAME)))
        : DEFAULT_RAFT_GROUP_ID;

    List<RaftPeer> peers = addresses.stream()
        .map(addr -> RaftPeer.newBuilder()
            .setId(RaftUtils.getPeerId(addr))
            .setAddress(addr)
            .build()
        ).collect(Collectors.toList());
    raftGroup = RaftGroup.valueOf(raftGroupIdFromConfig, peers);
    try (final RaftClient client = RaftUtils.createClient(raftGroup)) {
      final RaftGroupId remoteGroupId;
      if (raftGroupIdFromConfig != DEFAULT_RAFT_GROUP_ID) {
        remoteGroupId = raftGroupIdFromConfig;
      } else {
        final List<RaftGroupId> groupIds = run(peers,
            p -> client.getGroupManagementApi((p.getId())).list().getGroupIds());

        if (groupIds == null) {
          println("Failed to get group ID from " + peers);
          return -1;
        } else if (groupIds.size() == 1) {
          remoteGroupId = groupIds.get(0);
        } else {
          println("There are more than one groups, you should specific one. " + groupIds);
          return -2;
        }
      }

      groupInfoReply = run(peers, p -> client.getGroupManagementApi((p.getId())).info(remoteGroupId));
      processReply(groupInfoReply,
          () -> "Failed to get group info for group id " + remoteGroupId.getUuid() + " from " + peers);
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
    if (reply == null || !reply.isSuccess()) {
      final RaftException e = Optional.ofNullable(reply)
          .map(RaftClientReply::getException)
          .orElseGet(() -> new RaftException("Reply: " + reply));
      final String message = messageSupplier.get();
      printf("%s. Error: %s%n", message, e);
      throw new IOException(message, e);
    }
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
