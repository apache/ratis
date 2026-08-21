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
package org.apache.ratis.examples.counter.server;

import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.examples.common.Constants;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.quic.QuicConfigKeys;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.security.TlsConf;
import org.apache.ratis.security.TlsConf.CertificatesConf;
import org.apache.ratis.security.TlsConf.PrivateKeyConf;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.util.NetUtils;
import org.apache.ratis.util.TimeDuration;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Scanner;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Simplest Ratis server, use a simple state machine {@link CounterStateMachine}
 * which maintain a counter across multi server.
 * The single positional argument is the 0-based index of this server in
 * {@code raft.server.address.list} of the conf file resolved by {@link Constants}
 * (see the {@code RATIS_EXAMPLE_CONF} environment variable).
 * <p>
 * Run this application once per address on the list to set up a ratis cluster
 * which maintain a counter value replicated in each server memory
 * <p>
 * Pass {@code --quic} as the last argument to use QUIC transport instead of Netty.
 */
public final class CounterServer implements Closeable {
  private final RaftServer server;

  public CounterServer(RaftPeer peer, File storageDir, TimeDuration simulatedSlowness)
      throws IOException {
    this(peer, storageDir, simulatedSlowness, false);
  }

  public CounterServer(RaftPeer peer, File storageDir, TimeDuration simulatedSlowness,
      boolean useQuic) throws IOException {
    //create a property object
    final RaftProperties properties = new RaftProperties();

    //set the storage directory (different for each peer) in the RaftProperty object
    RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storageDir));

    // DEFAULT read policy — routes read-only requests to the leader (no server-to-server ReadIndex).
    // Same setting for QUIC and NETTY so the comparison is fair.

    //set the port (different for each peer) in RaftProperty object
    final int port = NetUtils.createSocketAddr(peer.getAddress()).getPort();

    //create the counter state machine which holds the counter value
    final CounterStateMachine counterStateMachine = new CounterStateMachine(simulatedSlowness);

    final Parameters parameters = new Parameters();

    if (useQuic) {
      RaftConfigKeys.Rpc.setType(properties, SupportedRpcType.QUIC);
      QuicConfigKeys.Server.setPort(properties, port);

      // Same CA-signed certs as Netty, so both transports do a real chain
      // verification handshake instead of skipping it — keeps the comparison fair.
      QuicConfigKeys.Server.setTlsCert(properties, "ratis-test/src/test/resources/ssl/server.crt");
      QuicConfigKeys.Server.setTlsKey(properties, "ratis-test/src/test/resources/ssl/server.pem");
      QuicConfigKeys.Client.setTlsCaCert(properties, "ratis-test/src/test/resources/ssl/ca.crt");
    } else {
      RaftConfigKeys.Rpc.setType(properties, SupportedRpcType.NETTY);
      NettyConfigKeys.Server.setPort(properties, port);

      NettyConfigKeys.Server.setTlsConf(parameters, new TlsConf.Builder()
          .setName("server")
          .setPrivateKey(new PrivateKeyConf(new File("ratis-test/src/test/resources/ssl/server.pem")))
          .setKeyCertificates(new CertificatesConf(new File("ratis-test/src/test/resources/ssl/server.crt")))
          .setTrustCertificates(new CertificatesConf(new File("ratis-test/src/test/resources/ssl/ca.crt")))
          .setMutualTls(false)
          .build());

      NettyConfigKeys.Client.setTlsConf(parameters, new TlsConf.Builder()
          .setName("server-as-client")
          .setPrivateKey(new PrivateKeyConf(new File("ratis-test/src/test/resources/ssl/client.pem")))
          .setKeyCertificates(new CertificatesConf(new File("ratis-test/src/test/resources/ssl/client.crt")))
          .setTrustCertificates(new CertificatesConf(new File("ratis-test/src/test/resources/ssl/ca.crt")))
          .setMutualTls(false)
          .build());
    }

    //build the Raft server
    this.server = RaftServer.newBuilder()
        .setGroup(Constants.RAFT_GROUP)
        .setProperties(properties)
        .setParameters(parameters)
        .setServerId(peer.getId())
        .setStateMachine(counterStateMachine)
        .setOption(RaftStorage.StartupOption.RECOVER)
        .build();
  }

  public void start() throws IOException {
    server.start();
  }

  @Override
  public void close() throws IOException {
    server.close();
  }

  public static void main(String[] args) {
    try {
      final List<String> argList = Arrays.asList(args);
      final boolean useQuic = argList.contains("--quic");
      final List<String> positional = argList.stream()
          .filter(a -> !a.startsWith("--"))
          .collect(java.util.stream.Collectors.toList());

      if (positional.size() != 1) {
        throw new IllegalArgumentException(
            "Invalid argument number: expected 1 positional argument but got " + positional.size());
      }
      final int peerIndex = Integer.parseInt(positional.get(0));
      final int numPeers = Constants.PEERS.size();
      if (peerIndex < 0 || peerIndex >= numPeers) {
        throw new IllegalArgumentException("The server index must be in [0, " + (numPeers - 1)
            + "] for the " + numPeers + " peer(s) configured in raft.server.address.list"
            + ": peerIndex=" + peerIndex);
      }
      TimeDuration simulatedSlowness = Optional.ofNullable(Constants.SIMULATED_SLOWNESS)
          .map(slownessList -> slownessList.get(peerIndex))
          .orElse(TimeDuration.ZERO);
      startServer(peerIndex, simulatedSlowness, useQuic);
    } catch(Throwable e) {
      e.printStackTrace();
      System.err.println();
      System.err.println("args = " + Arrays.toString(args));
      System.err.println();
      if (e instanceof IllegalArgumentException) {   // NumberFormatException is a subclass
        printUsage();
      } else {
        System.err.println("The failure above is NOT an argument problem"
            + " (conf file, storage, port or TLS?) - read the stack trace at the top.");
      }
      System.exit(1);
    }
  }

  private static void printUsage() {
    System.err.println("Usage: java " + CounterServer.class.getName() + " peer_index [--quic]");
    System.err.println();
    System.err.println("       peer_index 0-based index into raft.server.address.list"
        + " of the conf file (see RATIS_EXAMPLE_CONF)");
    try {
      System.err.println("       currently configured peers (" + Constants.PEERS.size() + "): "
          + Constants.PEERS);
    } catch (Throwable ignored) {
      System.err.println("       (peer list unavailable - the conf file could not be loaded)");
    }
    System.err.println("       --quic     use QUIC transport (default: Netty/TCP)");
  }

  private static void startServer(int peerIndex, TimeDuration simulatedSlowness,
      boolean useQuic) throws IOException {
    //get peer and define storage dir
    final RaftPeer currentPeer = Constants.PEERS.get(peerIndex);
    final File storageDir = new File("./" + currentPeer.getId());

    //start a counter server
    try(CounterServer counterServer = new CounterServer(
        currentPeer, storageDir, simulatedSlowness, useQuic)) {
      counterServer.start();

      //exit when any input entered
      new Scanner(System.in, UTF_8.name()).nextLine();
    }
  }
}
