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
package org.apache.raft.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.raft.MiniRaftCluster;
import org.apache.raft.conf.RaftProperties;
import org.apache.raft.grpc.MiniRaftClusterWithGRpc;
import org.apache.raft.grpc.server.PipelinedLogAppenderFactory;
import org.apache.raft.hadooprpc.MiniRaftClusterWithHadoopRpc;
import org.apache.raft.netty.MiniRaftClusterWithNetty;
import org.apache.raft.server.LogAppenderFactory;
import org.apache.raft.server.RaftServerConfigKeys;
import org.apache.raft.server.simulation.MiniRaftClusterWithSimulatedRpc;
import org.apache.raft.server.simulation.SimulatedRequestReply;
import org.apache.raft.statemachine.StateMachine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class RaftExamplesTestUtil {
  private static void add(Collection<Object[]> clusters, MiniRaftCluster c) {
    clusters.add(new Object[]{c});
  }

  public static Collection<Object[]> getMiniRaftClusters(
      RaftProperties prop, int clusterSize, Class<?>... clusterClasses)
      throws IOException {
    final List<Class<?>> classes = Arrays.asList(clusterClasses);
    final boolean isAll = classes.isEmpty(); //empty means all

    final String[][] ids = MiniRaftCluster.generateIds4MultiClusters(clusterSize, 4);
    int i = 0;

    final List<Object[]> clusters = new ArrayList<>();

    if (isAll || classes.contains(MiniRaftClusterWithSimulatedRpc.class)) {
      prop.setInt(SimulatedRequestReply.SIMULATE_LATENCY_KEY, 0);
      add(clusters, new MiniRaftClusterWithSimulatedRpc(ids[i++], prop, true));
    }
    if (isAll || classes.contains(MiniRaftClusterWithHadoopRpc.class)) {
      final Configuration conf = new Configuration();
      conf.set(RaftServerConfigKeys.Ipc.ADDRESS_KEY, "0.0.0.0:0");
      add(clusters, new MiniRaftClusterWithHadoopRpc(ids[i++], prop, conf, true));
    }
    if (isAll || classes.contains(MiniRaftClusterWithNetty.class)) {
      add(clusters, new MiniRaftClusterWithNetty(ids[i++], prop, true));
    }
    if (isAll || classes.contains(MiniRaftClusterWithGRpc.class)) {
      add(clusters, new MiniRaftClusterWithGRpc(ids[i++], prop, true));
    }
    return clusters;
  }

  public static <S extends StateMachine> Collection<Object[]> getMiniRaftClusters(
      Class<S> stateMachineClass, Class<?>... clusterClasses) throws IOException {
    final RaftProperties prop = new RaftProperties();
    prop.setClass(RaftServerConfigKeys.RAFT_SERVER_STATEMACHINE_CLASS_KEY,
        stateMachineClass, StateMachine.class);
    return RaftExamplesTestUtil.getMiniRaftClusters(prop, 3, clusterClasses);
  }
}
