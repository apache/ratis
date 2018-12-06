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
package org.apache.ratis;


import org.apache.log4j.Level;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.examples.ParameterizedBaseTest;
import org.apache.ratis.examples.arithmetic.ArithmeticStateMachine;
import org.apache.ratis.examples.arithmetic.TestArithmetic;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.server.impl.GroupManagementBaseTest;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.util.LogUtils;
import org.apache.ratis.util.function.CheckedBiConsumer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(Parameterized.class)
public class TestMultiRaftGroup extends BaseTest {
  static {
    LogUtils.setLogLevel(RaftServerImpl.LOG, Level.DEBUG);
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() throws IOException {
    return ParameterizedBaseTest.getMiniRaftClusters(ArithmeticStateMachine.class, 0);
  }

  @Parameterized.Parameter
  public MiniRaftCluster cluster;

  @Test
  public void testMultiRaftGroup() throws Exception {
    runTestMultiRaftGroup(3, 6, 9, 12, 15);
  }

  private void runTestMultiRaftGroup(int... idIndex) throws Exception {
    runTestMultiRaftGroup(idIndex, -1);
  }

  private final AtomicInteger start = new AtomicInteger(3);
  private final int count = 10;

  private void runTestMultiRaftGroup(int[] idIndex, int chosen) throws Exception {

    final CheckedBiConsumer<MiniRaftCluster, RaftGroup, IOException> checker = (cluster, group) -> {
      try (final RaftClient client = cluster.createClient(group)) {
        TestArithmetic.runTestPythagorean(client, start.getAndAdd(2*count), count);
      }
    };

    GroupManagementBaseTest.runMultiGroupTest(
        cluster, idIndex, chosen, checker);
  }
}
