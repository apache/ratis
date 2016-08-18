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
package org.apache.raft.hadoopRpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.raft.MiniRaftCluster;
import org.apache.raft.conf.RaftProperties;
import org.apache.raft.server.simulation.MiniRaftClusterWithSimulatedRpc;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

public class RaftHadoopRpcTestUtil {
  /** used by parameterized unit test */
  public static Collection<Object[]> getMiniRaftClusters(int clusterSize,
      Configuration hadoopConf, RaftProperties prop) throws IOException {
    String[] s1 = MiniRaftCluster.generateIds(clusterSize, 0);
    String[] s2 = MiniRaftCluster.generateIds(clusterSize, clusterSize);
    MiniRaftClusterWithSimulatedRpc c1 =
        new MiniRaftClusterWithSimulatedRpc(s1, prop, true);
    MiniRaftClusterWithHadoopRpc c2 =
        new MiniRaftClusterWithHadoopRpc(s2, prop, hadoopConf, true);
    return Arrays.asList(new Object[][]{{c1}, {c2}});
  }
}
