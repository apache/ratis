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
package org.apache.hadoop.raft;

import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

public class MiniRaftCluster {
  final List<RaftServer> servers;

  MiniRaftCluster(int numServers) {
    this.servers = new ArrayList<>();
    for(int i = 0; i < numServers; i++) {
      servers.add(new RaftServer("s" + i));
    }
  }

  void init() {
    final List<RaftServer> otherServers = new ArrayList<>(servers);
    for(RaftServer s : servers) {
      otherServers.remove(s);
      s.init(otherServers);
      otherServers.add(s);
    }
  }

  public void printServers(PrintStream out) {
    out.println("#servers = " + servers.size());
    for(RaftServer s : servers) {
      out.print("  ");
      out.println(s);
    }
  }
}
