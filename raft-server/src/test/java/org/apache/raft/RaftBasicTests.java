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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
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
  public static final Logger LOG = LoggerFactory.getLogger(RaftBasicTests.class);

  public static final int NUM_SERVERS = 5;

  protected static final RaftProperties properties = new RaftProperties();

  public abstract MiniRaftCluster getCluster();

  public RaftProperties getProperties() {
    return properties;
  }

  @Rule
  public Timeout globalTimeout = new Timeout(120 * 1000);

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
    LOG.info("Running testBasicLeaderElection");
    final MiniRaftCluster cluster = getCluster();
    waitAndKillLeader(cluster, true);
    waitAndKillLeader(cluster, true);
    waitAndKillLeader(cluster, true);
    waitAndKillLeader(cluster, false);
  }

  @Test
  public void testBasicAppendEntries() throws Exception {
    LOG.info("Running testBasicAppendEntries");
    final MiniRaftCluster cluster = getCluster();
    RaftServer leader = waitForLeader(cluster);
    final long term = leader.getState().getCurrentTerm();
    final String killed = cluster.getFollowers().get(3).getId();
    cluster.killServer(killed);
    LOG.info(cluster.printServers());

    final SimpleMessage[] messages = SimpleMessage.create(10);
    try(final RaftClient client = cluster.createClient("client", null)) {
      for (SimpleMessage message : messages) {
        client.send(message);
      }
    }

    Thread.sleep(cluster.getMaxTimeout() + 100);
    LOG.info(cluster.printAllLogs());

    cluster.getServers().stream().filter(RaftServer::isAlive)
        .map(s -> s.getState().getLog().getEntries(1, Long.MAX_VALUE))
        .forEach(e -> RaftTestUtil.assertLogEntries(e, 1, term, messages));
  }

  @Test
  public void testEnforceLeader() throws Exception {
    LOG.info("Running testEnforceLeader");
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
        client.close();
      } catch (IOException ioe) {
        exceptionInClientThread = ioe;
      }
    }
  }

  @Test
  public void testWithLoad() throws Exception {
    testWithLoad(10, 500);
  }

  private void testWithLoad(final int numClients, final int numMessages)
      throws Exception {
    LOG.info("Running testWithLoad: numClients=" + numClients
        + ", numMessages=" + numMessages);

    final MiniRaftCluster cluster = getCluster();
    LOG.info(cluster.printServers());

    final List<Client4TestWithLoad> clients
        = Stream.iterate(0, i -> i+1).limit(numClients)
        .map(i -> cluster.createClient(String.valueOf((char)('a' + i)), null))
        .map(c -> new Client4TestWithLoad(c, numMessages))
        .collect(Collectors.toList());
    clients.forEach(Thread::start);

    int count = 0;
    for(int lastStep = 0;; ) {
      if (clients.stream().filter(Client4TestWithLoad::isRunning).count() == 0) {
        break;
      }

      final int n = clients.stream().mapToInt(c -> c.step.get()).sum();
      if (n - lastStep < 50 * numClients) { // Change leader at least 50 steps.
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
