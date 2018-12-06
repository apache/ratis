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
import org.apache.ratis.RaftTestUtil.SimpleMessage;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.LogEntryProto.LogEntryBodyCase;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.LogAppender;
import org.apache.ratis.server.impl.ServerProtoUtils;
import org.apache.ratis.server.impl.ServerState;
import org.apache.ratis.server.storage.RaftLog;
import org.apache.ratis.statemachine.SimpleStateMachine4Testing;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.LogUtils;
import org.apache.ratis.util.SizeInBytes;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class LogAppenderTests<CLUSTER extends MiniRaftCluster>
    extends BaseTest
    implements MiniRaftCluster.Factory.Get<CLUSTER> {
  {
    LogUtils.setLogLevel(LogAppender.LOG, Level.DEBUG);
  }

  {
    final RaftProperties prop = getProperties();
    prop.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY, SimpleStateMachine4Testing.class, StateMachine.class);

    final SizeInBytes n = SizeInBytes.valueOf("8KB");
    RaftServerConfigKeys.Log.setSegmentSizeMax(prop, n);
    RaftServerConfigKeys.Log.Appender.setBufferByteLimit(prop, n);
  }

  static SimpleMessage[] generateMsgs(int num) {
    SimpleMessage[] msgs = new SimpleMessage[num * 6];
    for (int i = 0; i < num; i++) {
      for (int j = 0; j < 6; j++) {
        byte[] bytes = new byte[1024 * (j + 1)];
        Arrays.fill(bytes, (byte) (j + '0'));
        msgs[i * 6 + j] = new SimpleMessage(new String(bytes));
      }
    }
    return msgs;
  }

  private static class Sender extends Thread {
    private final RaftClient client;
    private final CountDownLatch latch;
    private final SimpleMessage[] messages;
    private final AtomicBoolean succeed = new AtomicBoolean(false);
    private final AtomicReference<Exception> exception = new AtomicReference<>();

    Sender(RaftClient client, int numMessages, CountDownLatch latch) {
      this.latch = latch;
      this.client = client;
      this.messages = generateMsgs(numMessages);
    }

    @Override
    public void run() {
      try {
        latch.await();
        for (SimpleMessage msg : messages) {
          client.send(msg);
        }
        client.close();
        succeed.set(true);
      } catch (Exception e) {
        exception.compareAndSet(null, e);
      }
    }
  }

  @Test
  public void testSingleElementBuffer() throws Exception {
    RaftServerConfigKeys.Log.Appender.setBufferElementLimit(getProperties(), 1);
    runWithNewCluster(3, this::runTest);
  }

  @Test
  public void testUnlimitedElementBuffer() throws Exception {
    RaftServerConfigKeys.Log.Appender.setBufferElementLimit(getProperties(), 0);
    runWithNewCluster(3, this::runTest);
  }

  void runTest(CLUSTER cluster) throws Exception {
    final int numMsgs = 10;
    final int numClients = 5;
    final RaftPeerId leaderId = RaftTestUtil.waitForLeader(cluster).getId();

    // start several clients and write concurrently
    final CountDownLatch latch = new CountDownLatch(1);
    final List<Sender> senders = Stream.iterate(0, i -> i+1).limit(numClients)
        .map(i -> new Sender(cluster.createClient(leaderId), numMsgs, latch))
        .collect(Collectors.toList());
    senders.forEach(Thread::start);

    latch.countDown();

    for (Sender s : senders) {
      s.join();
      final Exception e = s.exception.get();
      if (e != null) {
        throw e;
      }
      Assert.assertTrue(s.succeed.get());
    }

    final ServerState leaderState = cluster.getLeader().getState();
    final RaftLog leaderLog = leaderState.getLog();
    final EnumMap<LogEntryBodyCase, AtomicLong> counts = RaftTestUtil.countEntries(leaderLog);
    LOG.info("counts = " + counts);
    Assert.assertEquals(6 * numMsgs * numClients, counts.get(LogEntryBodyCase.STATEMACHINELOGENTRY).get());

    final LogEntryProto last = RaftTestUtil.getLastEntry(LogEntryBodyCase.STATEMACHINELOGENTRY, leaderLog);
    LOG.info("last = " + ServerProtoUtils.toLogEntryString(last));
    Assert.assertNotNull(last);
    Assert.assertTrue(last.getIndex() <= leaderState.getLastAppliedIndex());
  }
}
