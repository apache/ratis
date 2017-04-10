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
import org.apache.ratis.RaftTestUtil.SimpleMessage;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.util.LogUtils;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.examples.RaftExamplesTestUtil;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.simulation.RequestHandler;
import org.apache.ratis.statemachine.SimpleStateMachine4Testing;
import org.apache.ratis.statemachine.StateMachine;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Enable raft.server.log.appender.batch.enabled and test LogAppender
 */
@RunWith(Parameterized.class)
public class TestBatchAppend {
  static Logger LOG = LoggerFactory.getLogger(TestBatchAppend.class);
  static {
    LogUtils.setLogLevel(RaftServerImpl.LOG, Level.DEBUG);
    LogUtils.setLogLevel(RequestHandler.LOG, Level.DEBUG);
    LogUtils.setLogLevel(RaftClient.LOG, Level.DEBUG);
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() throws IOException {
    RaftProperties prop = new RaftProperties();
    prop.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        SimpleStateMachine4Testing.class, StateMachine.class);
    RaftServerConfigKeys.Log.setSegmentSizeMax(prop, SizeInBytes.valueOf("8KB"));
    // enable batch appending
    RaftServerConfigKeys.Log.Appender.setBatchEnabled(prop, true);
    // set batch appending buffer size to 4KB
    RaftServerConfigKeys.Log.Appender.setBufferCapacity(prop, SizeInBytes.valueOf("4KB"));

    return RaftExamplesTestUtil.getMiniRaftClusters(prop, 3);
  }

  @Parameterized.Parameter
  public MiniRaftCluster cluster;

  @Rule
  public Timeout globalTimeout = new Timeout(60 * 1000);

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private class Sender extends Thread {
    private final RaftClient client;
    private final CountDownLatch latch;
    private final SimpleMessage[] msgs;
    private final AtomicBoolean succeed = new AtomicBoolean(false);

    Sender(RaftPeerId leaderId, CountDownLatch latch, int numMsg) {
      this.latch = latch;
      this.client = cluster.createClient(leaderId);
      msgs = generateMsgs(numMsg);
    }

    SimpleMessage[] generateMsgs(int num) {
      SimpleMessage[] msgs = new SimpleMessage[num * 6];
      for (int i = 0; i < num; i++) {
        for (int j = 0; j < 6; j++) {
          byte[] bytes = new byte[1024 * (j + 1)];
          Arrays.fill(bytes, (byte) j);
          msgs[i * 6 + j] = new SimpleMessage(new String(bytes));
        }
      }
      return msgs;
    }

    @Override
    public void run() {
      try {
        latch.await();
      } catch (InterruptedException ignored) {
        LOG.warn("Client {} waiting for countdown latch got interrupted",
            client.getId());
      }
      for (SimpleMessage msg : msgs) {
        try {
          client.send(msg);
        } catch (IOException e) {
          succeed.set(false);
          LOG.warn("Client {} hit exception {}", client.getId(), e);
          return;
        }
      }
      succeed.set(true);
      try {
        client.close();
      } catch (IOException ignore) {
      }
    }
  }

  @Test
  public void testAppend() throws Exception {
    final int numMsgs = 10;
    final int numClients = 5;
    cluster.start();
    RaftTestUtil.waitForLeader(cluster);
    final RaftPeerId leaderId = cluster.getLeader().getId();

    // start several clients and write concurrently
    CountDownLatch latch = new CountDownLatch(1);
    final List<Sender> senders = Stream.iterate(0, i -> i+1).limit(numClients)
        .map(i -> new Sender(leaderId, latch, numMsgs))
        .collect(Collectors.toList());
    senders.forEach(Thread::start);

    latch.countDown();

    senders.forEach(sender -> {
      try {
        sender.join();
      } catch (InterruptedException ignored) {
      }
    });

    for (Sender s : senders) {
      Assert.assertTrue(s.succeed.get());
    }

    Assert.assertEquals(6 * numMsgs * numClients,
        cluster.getLeader().getState().getLastAppliedIndex());
  }
}
