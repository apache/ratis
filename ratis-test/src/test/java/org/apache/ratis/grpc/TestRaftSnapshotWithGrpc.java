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
package org.apache.ratis.grpc;

import java.util.Optional;

import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.metrics.MetricRegistries;
import org.apache.ratis.metrics.MetricRegistryInfo;
import org.apache.ratis.metrics.RatisMetricRegistry;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.statemachine.RaftSnapshotBaseTest;
import org.junit.Assert;

import com.codahale.metrics.Counter;

public class TestRaftSnapshotWithGrpc extends RaftSnapshotBaseTest {
  @Override
  public MiniRaftCluster.Factory<?> getFactory() {
    return MiniRaftClusterWithGrpc.FACTORY;
  }

  @Override
  protected void verifyInstallSnapshotMetric(RaftServer.Division leader) {
    MetricRegistryInfo info = new MetricRegistryInfo(leader.getMemberId().toString(),
        "ratis_grpc", "log_appender", "Metrics for Ratis Grpc Log Appender");
    Optional<RatisMetricRegistry> metricRegistry = MetricRegistries.global().get(info);
    Assert.assertTrue(metricRegistry.isPresent());
    Counter installSnapshotCounter = metricRegistry.get().counter("num_install_snapshot");
    Assert.assertNotNull(installSnapshotCounter);
    Assert.assertTrue(installSnapshotCounter.getCount() >= 1);
  }

}
