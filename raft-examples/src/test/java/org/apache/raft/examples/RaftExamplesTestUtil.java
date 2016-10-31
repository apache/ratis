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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

public class RaftExamplesTestUtil {
  public static Collection<Object[]> getMiniRaftClusters(int clusterSize,
      Configuration hadoopConf, RaftProperties prop) throws IOException {
    final String[][] ids = MiniRaftCluster.generateIds4MultiClusters(clusterSize, 4);
    final Object[][] clusters = {
        {new MiniRaftClusterWithSimulatedRpc(ids[0], prop, true)},
        {new MiniRaftClusterWithHadoopRpc(ids[1], prop, hadoopConf, true)},
        {null},
        {new MiniRaftClusterWithNetty(ids[3], prop, true)},
    };
    prop.setClass(RaftServerConfigKeys.RAFT_SERVER_LOG_APPENDER_FACTORY_CLASS_KEY,
        PipelinedLogAppenderFactory.class, LogAppenderFactory.class);
    clusters[2][0] = new MiniRaftClusterWithGRpc(ids[2], prop, true);
    return Arrays.asList(clusters);
  }
}
