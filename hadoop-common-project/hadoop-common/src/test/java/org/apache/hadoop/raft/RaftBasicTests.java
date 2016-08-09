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

import org.apache.hadoop.raft.RaftTestUtil.SimpleMessage;
import org.apache.hadoop.raft.client.RaftClient;
import org.apache.hadoop.raft.conf.RaftProperties;
import org.apache.hadoop.raft.server.RaftConstants;
import org.apache.hadoop.raft.server.RaftServer;
import org.apache.hadoop.raft.server.RaftServerConfigKeys;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.hadoop.raft.RaftTestUtil.waitAndKillLeader;
import static org.apache.hadoop.raft.RaftTestUtil.waitForLeader;

public abstract class RaftBasicTests {
  static final Logger LOG = LoggerFactory.getLogger(RaftBasicTests.class);

  public static final int NUM_SERVERS = 5;

  private final RaftProperties properties = new RaftProperties();

  {
    properties.setBoolean(RaftServerConfigKeys.RAFT_SERVER_USE_MEMORY_LOG_KEY, false);
  }

  public abstract MiniRaftCluster getCluster();

  public RaftProperties getProperties() {
    return properties;
  }

  @Before
  public void setup() throws IOException {
    Assert.assertNull(getCluster().getLeader());
    getCluster().start();
  }

  @After
  public void tearDown() {
    final MiniRaftCluster cluster = getCluster();
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testBasicLeaderElection() throws Exception {
    final MiniRaftCluster cluster = getCluster();
    waitAndKillLeader(cluster, true);
    waitAndKillLeader(cluster, true);
    waitAndKillLeader(cluster, true);
    waitAndKillLeader(cluster, false);
  }

  @Test
  public void testBasicAppendEntries() throws Exception {
    final MiniRaftCluster cluster = getCluster();
    RaftServer leader = waitForLeader(cluster);
    final long term = leader.getState().getCurrentTerm();
    final String killed = cluster.getFollowers().get(3).getId();
    cluster.killServer(killed);
    LOG.info(cluster.printServers());

    final RaftClient client = cluster.createClient("client", null);
    final SimpleMessage[] messages = SimpleMessage.create(10);
    for (int i = 0; i < messages.length; i++) {
      client.send(messages[i]);
    }

    Thread.sleep(RaftConstants.ELECTION_TIMEOUT_MAX_MS + 100);
    LOG.info(cluster.printAllLogs());

    cluster.getServers().stream().filter(RaftServer::isRunning)
        .map(s -> s.getState().getLog().getEntries(1, Long.MAX_VALUE))
        .forEach(e -> RaftTestUtil.assertLogEntries(e, 1, term, messages));
  }

  @Test
  public void testEnforceLeader() throws Exception {
    final String leader = "s" + ThreadLocalRandom.current().nextInt(NUM_SERVERS);
    LOG.info("enforce leader to " + leader);
    final MiniRaftCluster cluster = getCluster();
    waitForLeader(cluster);
    waitForLeader(cluster, leader);
  }

  @Test
  public void testWithLoad() throws Exception {
    final MiniRaftCluster cluster = getCluster();
    LOG.info(cluster.printServers());

    final SimpleMessage[] messages = SimpleMessage.create(1000);
    final RaftClient client = cluster.createClient("client", null);

    final Exception[] exceptionInClientThread = new Exception[1];
    final AtomicInteger step = new AtomicInteger();
    final Thread clientThread = new Thread(() -> {
      try {
        for (; step.get() < messages.length; step.incrementAndGet()) {
          client.send(messages[step.get()]);
        }
      } catch (IOException ioe) {
        exceptionInClientThread[0] = ioe;
      }
    });
    clientThread.start();

    int count = 0;
    for(int lastStep = 0;; ) {
      final int n = step.get();
      if (n >= messages.length) {
        break;
      } else if (n - lastStep < 50) { // Change leader at least 50 steps.
        Thread.sleep(10);
        continue;
      }
      lastStep = n;
      count++;

      RaftServer leader = cluster.getLeader();
      if (leader != null) {
        final String oldLeader = leader.getId();
        LOG.info("Block all requests sent by leader " + oldLeader);
        cluster.setBlockRequestsFrom(oldLeader, true);
        String newLeader = oldLeader;
        for(int i = 0; i < 10 && newLeader.equals(oldLeader); i++) {
          newLeader = RaftTestUtil.waitForLeader(cluster).getId();
        }
        cluster.setBlockRequestsFrom(oldLeader, false);
        LOG.info("Changed leader from " + oldLeader + " to " + newLeader);
        Assert.assertFalse(newLeader.equals(oldLeader));
      }
    }
    clientThread.join();
    if (exceptionInClientThread[0] != null) {
      throw new AssertionError(exceptionInClientThread[0]);
    }

    LOG.info("Leader change count=" + count + cluster.printAllLogs());

    RaftTestUtil.assertLogEntries(cluster.getServers(), messages);
  }
}

