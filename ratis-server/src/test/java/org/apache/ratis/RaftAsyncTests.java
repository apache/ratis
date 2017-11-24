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
package org.apache.ratis;

import org.apache.log4j.Level;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.impl.RaftClientTestUtil;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.impl.RaftServerProxy;
import org.apache.ratis.statemachine.SimpleStateMachine4Testing;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.LogUtils;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.ratis.RaftTestUtil.waitForLeader;

public abstract class RaftAsyncTests<CLUSTER extends MiniRaftCluster> extends BaseTest
    implements MiniRaftCluster.Factory.Get<CLUSTER> {
  static {
    LogUtils.setLogLevel(RaftServerImpl.LOG, Level.DEBUG);
    LogUtils.setLogLevel(RaftClient.LOG, Level.DEBUG);
  }

  {
    final RaftProperties p = getProperties();
    p.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        SimpleStateMachine4Testing.class, StateMachine.class);
  }

  private static final Logger LOG = LoggerFactory.getLogger(RaftAsyncTests.class);

  public static final int NUM_SERVERS = 5;

  private CLUSTER cluster = null;

  @Before
  public void setup() throws IOException {
    cluster = newCluster(NUM_SERVERS);
    Assert.assertNull(cluster.getLeader());
    cluster.start();
  }

  @After
  public void tearDown() {
    cluster.shutdown();
  }

  @Test
  public void testAsyncRequestSemaphore()
      throws InterruptedException, IOException {
    LOG.info("Running testBasicAppendEntries");
    waitForLeader(cluster);

    int numMessages = 100;
    CompletableFuture[] futures = new CompletableFuture[numMessages + 1];
    final RaftTestUtil.SimpleMessage[] messages = RaftTestUtil.SimpleMessage.create(numMessages);
    final RaftClient client = cluster.createClient();
    //Set blockTransaction flag so that transaction blocks
    for (RaftServerProxy server : cluster.getServers()) {
      ((SimpleStateMachine4Testing) server.getStateMachine()).setBlockTransaction(true);
    }

    //Send numMessages which are blocked and do not release the client semaphore permits
    AtomicInteger blockedRequestsCount = new AtomicInteger();
    for (int i=0; i<numMessages; i++) {
      blockedRequestsCount.getAndIncrement();
      futures[i] = client.sendAsync(messages[i]);
      blockedRequestsCount.decrementAndGet();
    }
    Assert.assertTrue(blockedRequestsCount.get() == 0);

    ExecutorService threadPool = Executors.newFixedThreadPool(1);
    futures[numMessages] = CompletableFuture.supplyAsync(() -> {
      blockedRequestsCount.incrementAndGet();
      client.sendAsync(new RaftTestUtil.SimpleMessage("n1"));
      blockedRequestsCount.decrementAndGet();
      return null;
    }, threadPool);

    //Allow the last msg to be sent
    while (blockedRequestsCount.get() != 1) {
      Thread.sleep(1000);
    }
    Assert.assertTrue(blockedRequestsCount.get() == 1);
    //Since all semaphore permits are acquired the last message sent is in queue
    RaftClientTestUtil.assertAsyncRequestSemaphore(client, 0, 1);

    //Unset the blockTransaction flag so that semaphore permits can be released
    for (RaftServerProxy server : cluster.getServers()) {
      ((SimpleStateMachine4Testing) server.getStateMachine()).setBlockTransaction(false);
    }
    for(int i=0; i<=numMessages; i++){
      futures[i].join();
    }
    Assert.assertTrue(blockedRequestsCount.get() == 0);
  }
}
