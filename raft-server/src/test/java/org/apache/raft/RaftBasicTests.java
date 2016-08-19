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
package org.apache.raft;

import org.apache.raft.RaftTestUtil.SimpleMessage;
import org.apache.raft.client.RaftClient;
import org.apache.raft.conf.RaftProperties;
import org.apache.raft.server.RaftServer;
import org.apache.raft.server.RaftServerConfigKeys;
import org.apache.raft.server.RaftServerConstants;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.raft.RaftTestUtil.waitAndKillLeader;
import static org.apache.raft.RaftTestUtil.waitForLeader;

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
    for (SimpleMessage message : messages) {
      client.send(message);
    }

    Thread.sleep(RaftServerConstants.ELECTION_TIMEOUT_MAX_MS + 100);
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

  static class Client4TestWithLoad extends Thread {
    final RaftClient client;
    final SimpleMessage[] messages;

    final AtomicInteger step = new AtomicInteger();
    volatile Exception exceptionInClientThread;

    Client4TestWithLoad(RaftClient client, int numMessages) {
      this.client = client;
      this.messages = SimpleMessage.create(numMessages, client.getId());
    }

    boolean isRunning() {
      return step.get() < messages.length && exceptionInClientThread == null;
    }

    @Override
    public void run() {
      try {
        for (; isRunning(); ) {
          client.send(messages[step.getAndIncrement()]);
        }
      } catch (IOException ioe) {
        exceptionInClientThread = ioe;
      }
    }
  }

  @Test
  public void testWithLoad() throws Exception {
    final int NUM_CLIENTS = 10;
    final int NUM_MESSAGES = 500;

    final MiniRaftCluster cluster = getCluster();
    LOG.info(cluster.printServers());

    final List<Client4TestWithLoad> clients
        = Stream.iterate(0, i -> i+1).limit(NUM_CLIENTS)
        .map(i -> cluster.createClient(String.valueOf((char)('a' + i)), null))
        .map(c -> new Client4TestWithLoad(c, NUM_MESSAGES))
        .collect(Collectors.toList());
    clients.stream().forEach(c -> c.start());

    int count = 0;
    for(int lastStep = 0;; ) {
      if (clients.stream().filter(c -> c.isRunning()).count() == 0) {
        break;
      }

      final int n = clients.stream().mapToInt(c -> c.step.get()).sum();
      if (n - lastStep < 50 * NUM_CLIENTS) { // Change leader at least 50 steps.
        Thread.sleep(10);
        continue;
      }
      lastStep = n;
      count++;

      RaftServer leader = cluster.getLeader();
      if (leader != null) {
        final String oldLeader = leader.getId();
        LOG.info("Block all requests sent by leader " + oldLeader);
        String newLeader = RaftTestUtil.changeLeader(cluster, oldLeader);
        LOG.info("Changed leader from " + oldLeader + " to " + newLeader);
        Assert.assertFalse(newLeader.equals(oldLeader));
      }
    }

    for(Client4TestWithLoad c : clients) {
      c.join();
    }
    for(Client4TestWithLoad c : clients) {
      if (c.exceptionInClientThread != null) {
        throw new AssertionError(c.exceptionInClientThread);
      }
      RaftTestUtil.assertLogEntries(cluster.getServers(), c.messages);
    }

    LOG.info("Leader change count=" + count + cluster.printAllLogs());

  }
}
