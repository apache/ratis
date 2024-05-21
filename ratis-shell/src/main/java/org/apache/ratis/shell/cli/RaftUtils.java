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
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.GrpcTlsConfig;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.protocol.*;
import org.apache.ratis.protocol.exceptions.RaftException;
import org.apache.ratis.retry.ExponentialBackoffRetry;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.function.CheckedFunction;

import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.*;
import java.util.stream.Collectors;

/**
 * Helper class for raft operations.
 */
public final class RaftUtils {

  public static final RaftGroupId DEFAULT_RAFT_GROUP_ID = RaftGroupId.randomId();


  private RaftUtils() {
    // prevent instantiation
  }

  /**
   * Gets the raft peer id.
   *
   * @param address the address of the server
   * @return the raft peer id
   */
  public static RaftPeerId getPeerId(InetSocketAddress address) {
    return getPeerId(address.getHostString(), address.getPort());
  }

  /**
   * Gets the raft peer id.
   *
   * @param host the hostname of the server
   * @param port the port of the server
   * @return the raft peer id
   */
  public static RaftPeerId getPeerId(String host, int port) {
    return RaftPeerId.getRaftPeerId(host + "_" + port);
  }

  /**
   * Create a raft client to communicate to ratis server.
   * @param raftGroup the raft group
   * @return return a raft client
   */
  public static RaftClient createClient(RaftGroup raftGroup) {
    return createClient(raftGroup, null, null);
  }


  /**
   * Create a raft client to communicate to ratis server.
   * @param raftGroup the raft group
   * @param rpcType the rpcType
   * @param tlsConfig the tlsConfig
   * @return return a raft client
   */
  public static RaftClient createClient(RaftGroup raftGroup, RpcType rpcType, GrpcTlsConfig tlsConfig) {
    RaftProperties properties = new RaftProperties();
    RaftClientConfigKeys.Rpc.setRequestTimeout(properties,
        TimeDuration.valueOf(15, TimeUnit.SECONDS));

    // Since ratis-shell support GENERIC_COMMAND_OPTIONS, here we should
    // merge these options to raft properties to make it work.
    final Properties sys = System.getProperties();
    sys.stringPropertyNames().forEach(key -> properties.set(key, sys.getProperty(key)));

    ExponentialBackoffRetry retryPolicy = ExponentialBackoffRetry.newBuilder()
        .setBaseSleepTime(TimeDuration.valueOf(1000, TimeUnit.MILLISECONDS))
        .setMaxAttempts(10)
        .setMaxSleepTime(
            TimeDuration.valueOf(100_000, TimeUnit.MILLISECONDS))
        .build();
    return RaftClient.newBuilder()
        .setRaftGroup(raftGroup)
        .setProperties(properties)
        .setRetryPolicy(retryPolicy)
        .setParameters(setClientTlsConf(rpcType, tlsConfig))
        .build();
  }

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
  public static <T, K, E extends Throwable> K runFunction(Collection<T> list, CheckedFunction<T, K, E> function) {
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


  public static List<RaftPeer> buildRaftPeersFromStr(String peers) {
    List<InetSocketAddress> addresses = new ArrayList<>();
    String[] peersArray = peers.split(",");
    for (String peer : peersArray) {
      addresses.add(parseInetSocketAddress(peer));
    }

    return addresses.stream()
        .map(addr -> RaftPeer.newBuilder()
            .setId(RaftUtils.getPeerId(addr))
            .setAddress(addr)
            .build()
        ).collect(Collectors.toList());
  }

  public static RaftGroupId buildRaftGroupIdFromStr(String groupId) {
    return (groupId != null && !groupId.equals("")) ? RaftGroupId.valueOf(UUID.fromString(groupId))
        : DEFAULT_RAFT_GROUP_ID;
  }

  public static RaftGroupId retrieveRemoteGroupId(RaftGroupId raftGroupIdFromConfig,
                                                  List<RaftPeer> peers,
                                                  RaftClient client, PrintStream printStream) throws IOException {
    RaftGroupId remoteGroupId;
    if (raftGroupIdFromConfig != DEFAULT_RAFT_GROUP_ID) {
      return raftGroupIdFromConfig;
    } else {
      final List<RaftGroupId> groupIds = runFunction(peers,
          p -> client.getGroupManagementApi((p.getId())).list().getGroupIds());

      if (groupIds == null) {
        printStream.println("Failed to get group ID from " + peers);
        throw new IOException("Failed to get group ID from " + peers);
      } else if (groupIds.size() == 1) {
        remoteGroupId = groupIds.get(0);
      } else {
        printStream.println("There are more than one groups, you should specific one. " + groupIds);
        throw new IOException("There are more than one groups, you should specific one. " + groupIds);
      }
    }

    return remoteGroupId;
  }

  public static GroupInfoReply retrieveGroupInfoByGroupId(RaftGroupId remoteGroupId, List<RaftPeer> peers, RaftClient client, PrintStream printStream)
      throws IOException {
    GroupInfoReply groupInfoReply = runFunction(peers, p -> client.getGroupManagementApi((p.getId())).info(remoteGroupId));
    processReply(groupInfoReply,
        printStream::println, "Failed to get group info for group id " + remoteGroupId.getUuid() + " from " + peers);
    return groupInfoReply;
  }

  public static void processReply(RaftClientReply reply, Consumer<String> printer, String message) throws IOException {
    processReplyInternal(reply, () -> printer.accept(message));
  }

  private static void processReplyInternal(RaftClientReply reply, Runnable printer) throws IOException {
    if (reply == null || !reply.isSuccess()) {
      final RaftException e = Optional.ofNullable(reply)
          .map(RaftClientReply::getException)
          .orElseGet(() -> new RaftException("Reply: " + reply));
      printer.run();
      throw new IOException(e.getMessage(), e);
    }
  }

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

  public static Parameters setClientTlsConf(RpcType rpcType,
                                            GrpcTlsConfig tlsConfig) {
    // TODO: GRPC TLS only for now, netty/hadoop RPC TLS support later.
    if (tlsConfig != null && rpcType == SupportedRpcType.GRPC) {
      Parameters parameters = new Parameters();
      setAdminTlsConf(parameters, tlsConfig);
      setClientTlsConf(parameters, tlsConfig);
      return parameters;
    }
    return null;
  }

  private static void setAdminTlsConf(Parameters parameters,
                                      GrpcTlsConfig tlsConfig) {
    if (tlsConfig != null) {
      GrpcConfigKeys.Admin.setTlsConf(parameters, tlsConfig);
    }
  }

  private static void setClientTlsConf(Parameters parameters,
                                       GrpcTlsConfig tlsConfig) {
    if (tlsConfig != null) {
      GrpcConfigKeys.Client.setTlsConf(parameters, tlsConfig);
      NettyConfigKeys.DataStream.Client.setTlsConf(parameters, tlsConfig);
    }
  }


}
