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
package org.apache.ratis.shell.cli;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.GroupInfoReply;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.RaftException;
import org.apache.ratis.util.function.CheckedFunction;

import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Utilities for command line interface.
 */
public final class CliUtils {
  private CliUtils() {
    // prevent instantiation
  }

  /** @return {@link RaftPeerId} from the given address. */
  public static RaftPeerId getPeerId(InetSocketAddress address) {
    return getPeerId(address.getHostString(), address.getPort());
  }

  /** @return {@link RaftPeerId} from the given host and port. */
  public static RaftPeerId getPeerId(String host, int port) {
    return RaftPeerId.getRaftPeerId(host + "_" + port);
  }

  /**
   * Apply the given function to the given parameter a list.
   *
   * @param list the input parameter list
   * @param function the function to be applied
   * @param <PARAMETER> parameter type
   * @param <RETURN> return value type
   * @param <EXCEPTION> the exception type thrown by the given function.
   * @return the first non-null value returned by the given function applied to the given list.
   */
  private static <PARAMETER, RETURN, EXCEPTION extends Throwable> RETURN applyFunctionReturnFirstNonNull(
      Collection<PARAMETER> list, CheckedFunction<PARAMETER, RETURN, EXCEPTION> function, PrintStream out) {
    for (PARAMETER parameter : list) {
      try {
        RETURN ret = function.apply(parameter);
        if (ret != null) {
          return ret;
        }
      } catch (Throwable e) {
        e.printStackTrace(out);
      }
    }
    return null;
  }

  /** Parse the given string as a list of {@link RaftPeer}. */
  public static List<RaftPeer> parseRaftPeers(String peers) {
    List<InetSocketAddress> addresses = new ArrayList<>();
    String[] peersArray = peers.split(",");
    for (String peer : peersArray) {
      addresses.add(parseInetSocketAddress(peer));
    }

    return addresses.stream()
        .map(addr -> RaftPeer.newBuilder().setId(getPeerId(addr)).setAddress(addr).build())
        .collect(Collectors.toList());
  }

  /** Parse the given string as a {@link RaftGroupId}. */
  public static RaftGroupId parseRaftGroupId(String groupId) {
    return groupId != null && groupId.isEmpty() ? RaftGroupId.valueOf(UUID.fromString(groupId)) : null;
  }

  /**
   * Get the group id from the given peers if the given group id is null.
   *
   * @param client for communicating to the peers.
   * @param peers the peers of the group.
   * @param groupId the given group id, if there is any.
   * @param err for printing error messages.
   * @return the group id from the given peers if the given group id is null;
   *         otherwise, return the given group id.
   */
  public static RaftGroupId getGroupId(RaftClient client, List<RaftPeer> peers, RaftGroupId groupId,
      PrintStream err) throws IOException {
    if (groupId != null) {
      return groupId;
    }

    final List<RaftGroupId> groupIds = applyFunctionReturnFirstNonNull(peers,
        p -> client.getGroupManagementApi(p.getId()).list().getGroupIds(), err);

    if (groupIds == null) {
      final String message = "Failed to get group ID from " + peers;
      err.println("Failed to get group ID from " + peers);
      throw new IOException(message);
    } else if (groupIds.size() == 1) {
      return groupIds.get(0);
    } else {
      String message = "Unexpected multiple group IDs " + groupIds
          + ".  In such case, the target group ID must be specified.";
      err.println(message);
      throw new IOException(message);
    }
  }

  /**
   * Get the group info from the given peers.
   *
   * @param client for communicating to the peers.
   * @param peers the peers of the group.
   * @param groupId the target group
   * @param err for printing error messages.
   * @return the group info
   */
  public static GroupInfoReply getGroupInfo(RaftClient client, List<RaftPeer> peers, RaftGroupId groupId,
      PrintStream err) throws IOException {
    GroupInfoReply groupInfoReply = applyFunctionReturnFirstNonNull(peers,
        p -> client.getGroupManagementApi((p.getId())).info(groupId), err);
    checkReply(groupInfoReply, () -> "Failed to get group info for " + groupId.getUuid()
            + " from " + peers, err);
    return groupInfoReply;
  }

  /** Check if the given reply is success. */
  public static void checkReply(RaftClientReply reply, Supplier<String> message, PrintStream printStream)
      throws IOException {
    if (reply == null || !reply.isSuccess()) {
      final RaftException e = Optional.ofNullable(reply)
          .map(RaftClientReply::getException)
          .orElseGet(() -> new RaftException("Reply: " + reply));
      printStream.println(message.get());
      throw new IOException(message.get(), e);
    }
  }

  /** Parse the given string as a {@link InetSocketAddress}. */
  public static InetSocketAddress parseInetSocketAddress(String address) {
    try {
      final String[] hostPortPair = address.split(":");
      if (hostPortPair.length < 2) {
        throw new IllegalArgumentException("Unexpected address format <HOST:PORT>.");
      }
      return new InetSocketAddress(hostPortPair[0], Integer.parseInt(hostPortPair[1]));
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to parse the server address parameter \"" + address + "\".", e);
    }
  }

}
