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
import org.apache.ratis.protocol.exceptions.TransferLeadershipException;
import org.apache.ratis.shell.cli.RaftUtils;
import org.apache.ratis.shell.cli.sh.command.AbstractRatisCommand;
import org.apache.ratis.shell.cli.sh.command.Context;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Command for transferring the ratis leader to specific server.
 */
public class TransferCommand extends AbstractRatisCommand {
  public static final String ADDRESS_OPTION_NAME = "address";
  public static final String TIMEOUT_OPTION_NAME = "timeout";
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
    // TODO: Default timeout should be set to 0, which means let server decide (based on election timeout).
    //       However, occasionally the request could timeout too fast while the transfer is in progress.
    //       i.e. request timeout doesn't mean transfer leadership has failed.
    //       Currently, Ratis shell returns merely based on the result of the request.
    //       So we set a larger default timeout here (3s).
    final long timeoutDefault = 3_000L;
    // Default timeout for legacy mode matches with the legacy command (version 2.4.x and older).
    final long timeoutLegacy = 60_000L;
    final Optional<Long> timeout = !cl.hasOption(TIMEOUT_OPTION_NAME) ? Optional.empty() :
        Optional.of(Long.parseLong(cl.getOptionValue(TIMEOUT_OPTION_NAME)) * 1000L);

    final int highestPriority = getRaftGroup().getPeers().stream()
        .mapToInt(RaftPeer::getPriority).max().orElse(0);
    RaftPeer newLeader = getRaftGroup().getPeers().stream()
        .filter(peer -> peer.getAddress().equals(strAddr)).findAny().orElse(null);
    if (newLeader == null) {
      return -2;
    }
    try (RaftClient client = RaftUtils.createClient(getRaftGroup())) {
      // transfer leadership
      Throwable err = tryTransfer(client, newLeader, highestPriority, timeout.orElse(timeoutDefault));
      if (err instanceof TransferLeadershipException
          && err.getMessage().contains("it does not has highest priority")) {
        // legacy mode, transfer leadership by setting priority.
        err = tryTransfer(client, newLeader, highestPriority + 1, timeout.orElse(timeoutLegacy));
      }
      if (err != null) {
        return -1;
      }
    }
    return 0;
  }

  private Throwable tryTransfer(RaftClient client, RaftPeer newLeader, int highestPriority, long timeout) {
    printf("Transferring leadership to server with address <%s> %n", newLeader.getAddress());
    try {
      // lift the current leader to the highest priority,
      if (newLeader.getPriority() < highestPriority) {
        setPriority(client, newLeader.getAddress(), highestPriority);
      }
      RaftClientReply transferLeadershipReply =
          client.admin().transferLeadership(newLeader.getId(), timeout);
      processReply(transferLeadershipReply, () -> "election failed");
    } catch (Throwable t) {
      printf("caught an error when executing transfer: %s%n", t.getMessage());
      return t;
    }
    println("Transferring leadership initiated");
    return null;
  }

  private void setPriority(RaftClient client, String address, int priority) throws IOException {
    printf("Changing priority of <%s> to %d%n: ", address, priority);
    List<RaftPeer> peers = getRaftGroup().getPeers().stream()
        .map(peer -> peer.getAddress().equals(address) ?
            RaftPeer.newBuilder(peer).setPriority(priority).build() : peer)
        .collect(Collectors.toList());
    RaftClientReply reply = client.admin().setConfiguration(peers);
    processReply(reply, () -> "Failed to set master priorities");
  }

  @Override
  public String getUsage() {
    return String.format("%s -%s <HOSTNAME:PORT>"
            + " -%s <PEER0_HOST:PEER0_PORT,PEER1_HOST:PEER1_PORT,PEER2_HOST:PEER2_PORT>"
            + " [-%s <RAFT_GROUP_ID>] [-%s <TIMEOUT_IN_SECONDS>]",
        getCommandName(), ADDRESS_OPTION_NAME, PEER_OPTION_NAME,
        GROUPID_OPTION_NAME, TIMEOUT_OPTION_NAME);
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
    ).addOption(
        Option.builder()
            .option(TIMEOUT_OPTION_NAME)
            .hasArg()
            .desc("Timeout for transfer leadership to complete (in seconds)")
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
