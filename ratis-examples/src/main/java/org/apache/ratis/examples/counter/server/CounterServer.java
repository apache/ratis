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
import org.apache.ratis.util.SizeInBytes;
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
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

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
 * Add {@code --single-stream} (QUIC only) to carry every server-to-server message type on
 * one QUIC stream per connection instead of one stream per type; that is the benchmark
 * baseline for the per-type stream layout ({@code raft.quic.server.single-stream}).
 */
public final class CounterServer implements Closeable {
  private final RaftServer server;

  public CounterServer(RaftPeer peer, File storageDir, TimeDuration simulatedSlowness)
      throws IOException {
    this(peer, storageDir, simulatedSlowness, false);
  }

  public CounterServer(RaftPeer peer, File storageDir, TimeDuration simulatedSlowness,
      boolean useQuic) throws IOException {
    this(peer, storageDir, simulatedSlowness, useQuic, false);
  }

  /** @param quicSingleStream QUIC only: one stream per server-to-server connection instead
   *  of one per message type ({@code raft.quic.server.single-stream}), see --single-stream. */
  public CounterServer(RaftPeer peer, File storageDir, TimeDuration simulatedSlowness,
      boolean useQuic, boolean quicSingleStream) throws IOException {
    this(peer, storageDir, simulatedSlowness, useQuic, quicSingleStream, false);
  }

  /** @param heartbeatThread both transports: heartbeats from a dedicated thread, sent next to
   *  an in-flight AppendEntries/InstallSnapshot ({@code raft.server.log.appender.heartbeat.thread}),
   *  see --hb-thread and HB-THREAD-CHANGES.md. Default false = stock appender. */
  public CounterServer(RaftPeer peer, File storageDir, TimeDuration simulatedSlowness,
      boolean useQuic, boolean quicSingleStream, boolean heartbeatThread) throws IOException {
    this(peer, storageDir, simulatedSlowness, useQuic, quicSingleStream, heartbeatThread, null);
  }

  /** @param extraProperties optional server configuration applied after the defaults; carries the
   *  benchmark flags --rpc-timeout=MIN,MAX and --no-prevote (HB-THREAD-CHANGES.md). May be null. */
  public CounterServer(RaftPeer peer, File storageDir, TimeDuration simulatedSlowness,
      boolean useQuic, boolean quicSingleStream, boolean heartbeatThread,
      Consumer<RaftProperties> extraProperties) throws IOException {
    //create a property object
    final RaftProperties properties = new RaftProperties();

    //set the storage directory (different for each peer) in the RaftProperty object
    RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storageDir));

    // Heartbeat thread option (HB-THREAD-CHANGES.md): off unless --hb-thread was given.
    if (heartbeatThread) {
      RaftServerConfigKeys.Log.Appender.setHeartbeatThread(properties, true);
    }
    // Election-timeout range and pre-vote flags (HB-THREAD-CHANGES.md): no-op unless given.
    if (extraProperties != null) {
      extraProperties.accept(properties);
    }

    // TYMCZASOWE, DO EKSPERYMENTU - USUNAC PO ZAKONCZENIU POMIAROW.
    // Ile bajtow wpisow logu lider pakuje w JEDNO AppendEntries. Domyslna wartosc jest
    // identyczna z domyslna Ratisa (4MB), wiec bez podania -Dratis.appender.buffer nic sie
    // nie zmienia. Sluzy do sprawdzenia, czy zapasc przy 1MB bierze sie stad, ze przesylka
    // ucisza lacze do followera dluzej niz okno elekcji (150-300 ms).
    // UWAGA: wartosc musi byc WIEKSZA niz najwiekszy pojedynczy wpis - DataQueue.offer ma
    // assertTrue(elementNumBytes <= byteLimit) i przy mniejszej rzuca wyjatkiem.
    RaftServerConfigKeys.Log.Appender.setBufferByteLimit(properties,
        SizeInBytes.valueOf(System.getProperty("ratis.appender.buffer", "4MB")));
    // Bufor zapisu logu (raft.server.log.write.buffer.size, domyslnie 8MB) musi byc wiekszy niz
    // limit paczki + 8 B, inaczej SegmentedRaftLogWorker odmawia startu. -Dratis.log.write.buffer
    // pozwala podniesc go razem z -Dratis.appender.buffer (np. 16MB -> 32MB). HB-THREAD-CHANGES.md.
    final String writeBuffer = System.getProperty("ratis.log.write.buffer");
    if (writeBuffer != null) {
      RaftServerConfigKeys.Log.setWriteBufferSize(properties, SizeInBytes.valueOf(writeBuffer));
    }

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

      // Stream layout of server-to-server connections: default = one stream per message type.
      if (quicSingleStream) {
        QuicConfigKeys.Server.setSingleStream(properties, true);
      }
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
      final boolean quicSingleStream = argList.contains("--single-stream");
      if (quicSingleStream && !useQuic) {
        throw new IllegalArgumentException("--single-stream applies to QUIC only: add --quic");
      }
      final boolean heartbeatThread = argList.contains("--hb-thread");
      // --rpc-timeout=MIN,MAX (ms): election timeout range for this server (Ratis derives the
      // heartbeat interval as MIN/2). --no-prevote: classic Raft without the pre-vote phase.
      // Both are one-token flags so the positional filter below keeps working.
      final String rpcTimeout = argList.stream().filter(a -> a.startsWith("--rpc-timeout="))
          .map(a -> a.substring("--rpc-timeout=".length())).findFirst().orElse(null);
      long rpcTimeoutMin = -1;
      long rpcTimeoutMax = -1;
      if (rpcTimeout != null) {
        final String[] mm = rpcTimeout.split(",");
        if (mm.length != 2) {
          throw new IllegalArgumentException("--rpc-timeout=MIN,MAX (ms) expected, got: " + rpcTimeout);
        }
        rpcTimeoutMin = Long.parseLong(mm[0].trim());
        rpcTimeoutMax = Long.parseLong(mm[1].trim());
        if (rpcTimeoutMin <= 0 || rpcTimeoutMax < rpcTimeoutMin) {
          throw new IllegalArgumentException("--rpc-timeout: need 0 < MIN <= MAX, got: " + rpcTimeout);
        }
      }
      final long tMin = rpcTimeoutMin;
      final long tMax = rpcTimeoutMax;
      final boolean noPreVote = argList.contains("--no-prevote");
      final Consumer<RaftProperties> extra = p -> {
        if (tMin > 0) {
          RaftServerConfigKeys.Rpc.setTimeoutMin(p, TimeDuration.valueOf(tMin, TimeUnit.MILLISECONDS));
          RaftServerConfigKeys.Rpc.setTimeoutMax(p, TimeDuration.valueOf(tMax, TimeUnit.MILLISECONDS));
        }
        if (noPreVote) {
          RaftServerConfigKeys.LeaderElection.setPreVote(p, false);
        }
      };
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
      startServer(peerIndex, simulatedSlowness, useQuic, quicSingleStream, heartbeatThread, extra);
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
    System.err.println("Usage: java " + CounterServer.class.getName()
        + " peer_index [--quic [--single-stream]] [--hb-thread] [--rpc-timeout=MIN,MAX] [--no-prevote]");
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
    System.err.println("       --single-stream  (with --quic) one QUIC stream per server-to-server"
        + " connection instead of one stream per message type");
    System.err.println("       --hb-thread      (both transports) heartbeats from a dedicated thread,"
        + " sent next to an in-flight AppendEntries/InstallSnapshot"
        + " (raft.server.log.appender.heartbeat.thread=true)");
    System.err.println("       --rpc-timeout=MIN,MAX  election timeout range in ms"
        + " (raft.server.rpc.timeout.min/max; heartbeat interval = MIN/2), default 150,300");
    System.err.println("       --no-prevote     classic Raft without the pre-vote phase"
        + " (raft.server.leaderelection.pre-vote=false)");
  }

  private static void startServer(int peerIndex, TimeDuration simulatedSlowness,
      boolean useQuic, boolean quicSingleStream, boolean heartbeatThread,
      Consumer<RaftProperties> extraProperties) throws IOException {
    //get peer and define storage dir
    final RaftPeer currentPeer = Constants.PEERS.get(peerIndex);
    final File storageDir = new File("./" + currentPeer.getId());

    //start a counter server
    try(CounterServer counterServer = new CounterServer(
        currentPeer, storageDir, simulatedSlowness, useQuic, quicSingleStream, heartbeatThread,
        extraProperties)) {
      counterServer.start();

      //exit when any input entered
      new Scanner(System.in, UTF_8.name()).nextLine();
    }
  }
}
