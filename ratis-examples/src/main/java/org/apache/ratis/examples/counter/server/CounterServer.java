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

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.examples.common.Constants;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.RaftPeer;
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
import java.util.Optional;
import java.util.Scanner;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Simplest Ratis server, use a simple state machine {@link CounterStateMachine}
 * which maintain a counter across multi server.
 * This server application designed to run several times with different
 * parameters (1,2 or 3). server addresses hard coded in {@link Constants}
 * <p>
 * Run this application three times with three different parameter set-up a
 * ratis cluster which maintain a counter value replicated in each server memory
 */
public final class CounterServer implements Closeable {
  private final RaftServer server;

  public CounterServer(RaftPeer peer, File storageDir, TimeDuration simulatedSlowness) throws IOException {
    //create a property object
    final RaftProperties properties = new RaftProperties();

    //set the storage directory (different for each peer) in the RaftProperty object
    RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storageDir));

    //set the read policy to Linearizable Read.
    //the Default policy will route read-only requests to leader and directly query leader statemachine.
    //Linearizable Read allows to route read-only requests to any group member
    //and uses ReadIndex to guarantee strong consistency.
    RaftServerConfigKeys.Read.setOption(properties, RaftServerConfigKeys.Read.Option.LINEARIZABLE);
    //set the linearizable read timeout
    RaftServerConfigKeys.Read.setTimeout(properties, TimeDuration.ONE_MINUTE);

    //set the port (different for each peer) in RaftProperty object
    final int port = NetUtils.createSocketAddr(peer.getAddress()).getPort();
    GrpcConfigKeys.Server.setPort(properties, port);

    //create the counter state machine which holds the counter value
    final CounterStateMachine counterStateMachine = new CounterStateMachine(simulatedSlowness);

    //build the Raft server
    this.server = RaftServer.newBuilder()
        .setGroup(Constants.RAFT_GROUP)
        .setProperties(properties)
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
      //get peerIndex from the arguments
      if (args.length != 1) {
        throw new IllegalArgumentException("Invalid argument number: expected to be 1 but actual is " + args.length);
      }
      final int peerIndex = Integer.parseInt(args[0]);
      if (peerIndex < 0 || peerIndex > 2) {
        throw new IllegalArgumentException("The server index must be 0, 1 or 2: peerIndex=" + peerIndex);
      }
      TimeDuration simulatedSlowness = Optional.ofNullable(Constants.SIMULATED_SLOWNESS)
                  .map(slownessList -> slownessList.get(peerIndex))
                  .orElse(TimeDuration.ZERO);
      startServer(peerIndex, simulatedSlowness);
    } catch(Throwable e) {
      e.printStackTrace();
      System.err.println();
      System.err.println("args = " + Arrays.toString(args));
      System.err.println();
      System.err.println("Usage: java org.apache.ratis.examples.counter.server.CounterServer peer_index");
      System.err.println();
      System.err.println("       peer_index must be 0, 1 or 2");
      System.exit(1);
    }
  }

  private static void startServer(int peerIndex, TimeDuration simulatedSlowness) throws IOException {
    //get peer and define storage dir
    final RaftPeer currentPeer = Constants.PEERS.get(peerIndex);
    final File storageDir = new File("./" + currentPeer.getId());

    //start a counter server
    try(CounterServer counterServer = new CounterServer(currentPeer, storageDir, simulatedSlowness)) {
      counterServer.start();

      //exit when any input entered
      new Scanner(System.in, UTF_8.name()).nextLine();
    }
  }
}
