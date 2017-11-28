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
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.client.impl.RaftClientTestUtil;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.impl.RaftServerProxy;
import org.apache.ratis.statemachine.SimpleStateMachine4Testing;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.LogUtils;
import org.junit.*;

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

  private RaftProperties properties;

  public static final int NUM_SERVERS = 3;

  @Before
  public void setup() throws IOException {
    properties = new RaftProperties();
    properties.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        SimpleStateMachine4Testing.class, StateMachine.class);
  }

  @Test
  public void testAsyncConfiguration() throws IOException {
    LOG.info("Running testAsyncConfiguration");
    RaftClient.Builder clientBuilder = RaftClient.newBuilder()
        .setRaftGroup(RaftGroup.emptyGroup())
        .setProperties(properties);
    int numThreads = RaftClientConfigKeys.Async.SCHEDULER_THREADS_DEFAULT;
    int maxOutstandingRequests = RaftClientConfigKeys.Async.MAX_OUTSTANDING_REQUESTS_DEFAULT;
    try(RaftClient client = clientBuilder.build()) {
      RaftClientTestUtil.assertScheduler(client, numThreads);
      RaftClientTestUtil.assertAsyncRequestSemaphore(client, maxOutstandingRequests, 0);
    }

    numThreads = 200;
    maxOutstandingRequests = 5;
    RaftClientConfigKeys.Async.setMaxOutstandingRequests(properties, maxOutstandingRequests);
    RaftClientConfigKeys.Async.setSchedulerThreads(properties, numThreads);
    try(RaftClient client = clientBuilder.build()) {
      RaftClientTestUtil.assertScheduler(client, numThreads);
      RaftClientTestUtil.assertAsyncRequestSemaphore(client, maxOutstandingRequests, 0);
    }

    // reset to default for other tests.
    RaftClientConfigKeys.Async.setMaxOutstandingRequests(properties,
        RaftClientConfigKeys.Async.MAX_OUTSTANDING_REQUESTS_DEFAULT);
    RaftClientConfigKeys.Async.setSchedulerThreads(properties,
        RaftClientConfigKeys.Async.SCHEDULER_THREADS_DEFAULT);
  }

  @Test
  public void testAsyncRequestSemaphore()
      throws InterruptedException, IOException {
    LOG.info("Running testAsyncRequestSemaphore");
    CLUSTER cluster = getFactory().newCluster(NUM_SERVERS, properties);
    Assert.assertNull(cluster.getLeader());
    cluster.start();
    waitForLeader(cluster);

    int numMessages = RaftClientConfigKeys.Async.maxOutstandingRequests(properties);
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
    cluster.shutdown();
  }

  @Test
  public void testBasicAppendEntriesAsync() throws Exception {
    LOG.info("Running testBasicAppendEntriesAsync");
    final CLUSTER cluster = getFactory().newCluster(NUM_SERVERS, properties);
    cluster.start();
    waitForLeader(cluster);
    RaftBasicTests.runTestBasicAppendEntries(true, 10, cluster, LOG);
    cluster.shutdown();
  }
}
