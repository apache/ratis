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
package org.apache.ratis.examples;

import org.apache.ratis.BaseTest;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.MiniRaftClusterWithGrpc;
import org.apache.ratis.netty.MiniRaftClusterWithNetty;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.simulation.MiniRaftClusterWithSimulatedRpc;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.TimeDuration;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@RunWith(Parameterized.class)
public abstract class ParameterizedBaseTest extends BaseTest {
  public static final Logger LOG = LoggerFactory.getLogger(ParameterizedBaseTest.class);

  /** Subclasses should override this method to provide real data parameters. */
  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Collections.emptyList();
  }

  /** For {@link Parameterized} test so that a cluster can be shared by multiple {@link Test} */
  private static final AtomicReference<MiniRaftCluster> currentCluster = new AtomicReference<>();

  /** Set {@link #currentCluster} to the given cluster and start it if {@link #currentCluster} is changed. */
  public static void setAndStart(MiniRaftCluster cluster) throws InterruptedException, IOException {
    final MiniRaftCluster previous = currentCluster.getAndSet(cluster);
    if (previous != cluster) {
      if (previous != null) {
        previous.shutdown();
      }

      cluster.start();
      RaftTestUtil.waitForLeader(cluster);
    }
  }

  @AfterClass
  public static void shutdownCurrentCluster() {
    final MiniRaftCluster cluster = currentCluster.getAndSet(null);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private static void add(
      Collection<Object[]> clusters, MiniRaftCluster.Factory factory,
      String[] ids, RaftProperties properties) {
    clusters.add(new Object[]{factory.newCluster(ids, properties)});
  }

  public static Collection<Object[]> getMiniRaftClusters(
      RaftProperties prop, int clusterSize, Class<?>... clusterClasses) {
    final List<Class<?>> classes = Arrays.asList(clusterClasses);
    final boolean isAll = classes.isEmpty(); //empty means all

    final Iterator<String[]> ids = new Iterator<String[]>() {
      private int i = 0;
      @Override
      public boolean hasNext() {
        return true;
      }
      @Override
      public String[] next() {
        return MiniRaftCluster.generateIds(clusterSize, i++*clusterSize);
      }
    };

    final List<Object[]> clusters = new ArrayList<>();

    if (isAll || classes.contains(MiniRaftClusterWithSimulatedRpc.class)) {
      add(clusters, MiniRaftClusterWithSimulatedRpc.FACTORY, ids.next(), prop);
    }
    if (isAll || classes.contains(MiniRaftClusterWithGrpc.class)) {
      add(clusters, MiniRaftClusterWithGrpc.FACTORY, ids.next(), prop);
    }
    if (isAll || classes.contains(MiniRaftClusterWithNetty.class)) {
      add(clusters, MiniRaftClusterWithNetty.FACTORY, ids.next(), prop);
    }
    for(int i = 0; i < clusters.size(); i++) {
      LOG.info(i + ": " + JavaUtils.getClassSimpleName(clusters.get(i)[0].getClass()));
    }
    LOG.info("#clusters = " + clusters.size());
    return clusters;
  }

  public static <S extends StateMachine> Collection<Object[]> getMiniRaftClusters(
      Class<S> stateMachineClass, int clusterSize, Class<?>... clusterClasses) {
    final RaftProperties prop = new RaftProperties();

    // avoid flaky behaviour in CI environment
    RaftServerConfigKeys.Rpc.setTimeoutMin(prop, TimeDuration.valueOf(300, TimeUnit.MILLISECONDS));
    RaftServerConfigKeys.Rpc.setTimeoutMax(prop, TimeDuration.valueOf(600, TimeUnit.MILLISECONDS));

    prop.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        stateMachineClass, StateMachine.class);
    return getMiniRaftClusters(prop, clusterSize, clusterClasses);
  }
}
