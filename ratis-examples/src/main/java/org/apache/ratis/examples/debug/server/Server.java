/**
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

package org.apache.ratis.examples.debug.server;

import org.apache.ratis.examples.common.Constants;
import org.apache.ratis.examples.counter.server.CounterServer;
import org.apache.ratis.protocol.RaftPeer;

import java.io.File;
import java.io.IOException;

/**
 * For running a {@link CounterServer}.
 */
public final class Server {

  private Server(){
  }

  public static void main(String[] args) throws IOException {
    if (args.length < 1) {
      System.err.println("The arguments should be <ip:port>");
      System.exit(1);
    }

    //find current peer object based on application parameter
    final RaftPeer currentPeer = Constants.PEERS.stream()
        .filter(raftPeer -> raftPeer.getAddress().equals(args[0]))
        .findFirst().orElseThrow(() -> new IllegalArgumentException("Peer not found: " + args[0]));

    final File storageDir = new File(Constants.PATH, currentPeer.getId().toString());
    final CounterServer counterServer = new CounterServer(currentPeer, storageDir);
    counterServer.start();
  }
}
