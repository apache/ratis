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
package org.apache.ratis.logservice;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.ratis.BaseTest;
import org.apache.ratis.MiniRaftCluster;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.logservice.api.LogName;
import org.apache.ratis.logservice.api.LogService;
import org.apache.ratis.logservice.api.LogServiceConfiguration;
import org.apache.ratis.logservice.api.LogStateMachine;
import org.apache.ratis.logservice.api.LogStream;
import org.apache.ratis.logservice.api.LogStream.State;
import org.apache.ratis.statemachine.StateMachine;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class LogServiceBaseTest<CLUSTER extends MiniRaftCluster>
    extends BaseTest
    implements MiniRaftCluster.Factory.Get<CLUSTER> {
  public static final Logger LOG = LoggerFactory.getLogger(LogServiceBaseTest.class);

  {
    final RaftProperties p = getProperties();
    p.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        LogStateMachine.class, StateMachine.class);
    LOG.info("Set LogStateMachine OK");
  }

  static final int NUM_PEERS = 3;
  CLUSTER cluster;

  @Before
  public void setUpCluster() throws IOException, InterruptedException {
    cluster = newCluster(NUM_PEERS);
    cluster.start();
    RaftTestUtil.waitForLeader(cluster);
  }

  @Test
  public void testLogServiceAdminAPIs() throws Exception {
    RaftClient raftClient =
        RaftClient.newBuilder().setProperties(getProperties()).setRaftGroup(cluster.getGroup())
            .build();
    LogService logService = LogServiceFactory.getInstance().createLogService(raftClient,
      new LogServiceConfiguration());
    LogName logName = LogName.of("log1");
    LogStream logStream = logService.createLog(logName);
    assertEquals("log1", logStream.getName().getName());
    assertEquals(State.OPEN, logStream.getState());
    assertEquals(0, logStream.getSize());
    logService.getLog(logName);
    assertEquals("log1", logStream.getName().getName());
    assertEquals(State.OPEN, logStream.getState());
    assertEquals(0, logStream.getSize());
    // TODO fix me
    //    logStream = logService.listLogs().next();
    //    assertEquals("log1", logStream.getName().getName());
    //    assertEquals(State.OPEN, logStream.getState());
    //    assertEquals(0, logStream.getSize());
    State state = logService.getState(logName);
    assertEquals(State.OPEN, state);
  }

  @After
  public void tearDown() {
    cluster.shutdown();
  }
}
