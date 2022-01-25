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

import org.apache.ratis.client.impl.RaftOutputStream;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.LogEntryProto.LogEntryBodyCase;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.LogEntryHeader;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.StringUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.apache.ratis.RaftTestUtil.waitForLeader;
import static org.junit.Assert.fail;

public abstract class OutputStreamBaseTest<CLUSTER extends MiniRaftCluster>
    extends BaseTest
    implements MiniRaftCluster.Factory.Get<CLUSTER> {
  private static final int NUM_SERVERS = 3;
  private static final byte[] BYTES = new byte[4];

  public OutputStream newOutputStream(CLUSTER cluster, int bufferSize) {
    return new RaftOutputStream(cluster::createClient, SizeInBytes.valueOf(bufferSize));
  }

  private static byte[] toBytes(int i) {
    final byte[] b = BYTES;
    b[0] = (byte) ((i >>> 24) & 0xFF);
    b[1] = (byte) ((i >>> 16) & 0xFF);
    b[2] = (byte) ((i >>> 8) & 0xFF);
    b[3] = (byte) (i & 0xFF);
    return b;
  }

  @Test
  public void testSimpleWrite() throws Exception {
    runWithNewCluster(NUM_SERVERS, this::runTestSimpleWrite);
  }

  private void runTestSimpleWrite(CLUSTER cluster) throws Exception {
    final int numRequests = 5000;
    final int bufferSize = 4;
    RaftTestUtil.waitForLeader(cluster);
    try (OutputStream out = newOutputStream(cluster, bufferSize)) {
      for (int i = 0; i < numRequests; i++) { // generate requests
        out.write(toBytes(i));
      }
    }

    // check the leader's raft log
    final RaftLog raftLog = cluster.getLeader().getRaftLog();
    final AtomicInteger i = new AtomicInteger();
    checkLog(raftLog, numRequests, () -> toBytes(i.getAndIncrement()));
  }

  private void checkLog(RaftLog raftLog, long expectedCommittedIndex,
      Supplier<byte[]> s) throws IOException {
    long committedIndex = raftLog.getLastCommittedIndex();
    Assert.assertTrue(committedIndex >= expectedCommittedIndex);
    // check the log content
    final LogEntryHeader[] entries = raftLog.getEntries(0, Long.MAX_VALUE);
    int count = 0;
    for (LogEntryHeader entry : entries) {
      LogEntryProto log  = raftLog.get(entry.getIndex());
      if (!log.hasStateMachineLogEntry()) {
        continue;
      }
      byte[] logData = log.getStateMachineLogEntry().getLogData().toByteArray();
      byte[] expected = s.get();
      final String message = "log " + entry + " " + log.getLogEntryBodyCase()
          + " " + StringUtils.bytes2HexString(logData)
          + ", expected=" + StringUtils.bytes2HexString(expected);
      Assert.assertArrayEquals(message, expected, logData);
      count++;
    }
    Assert.assertEquals(expectedCommittedIndex, count);
  }

  @Test
  public void testWriteAndFlush() throws Exception {
    runWithNewCluster(NUM_SERVERS, this::runTestWriteAndFlush);
  }

  private void runTestWriteAndFlush(CLUSTER cluster) throws Exception {
    final int bufferSize = ByteValue.BUFFERSIZE;
    final RaftServer.Division leader = waitForLeader(cluster);
    OutputStream out = newOutputStream(cluster, bufferSize);

    int[] lengths = new int[]{1, 500, 1023, 1024, 1025, 2048, 3000, 3072};
    ByteValue[] values = new ByteValue[lengths.length];
    for (int i = 0; i < values.length; i++) {
      values[i] = new ByteValue(lengths[i], (byte) 9);
    }

    List<byte[]> expectedTxs = new ArrayList<>();
    for (ByteValue v : values) {
      byte[] data = v.genData();
      expectedTxs.addAll(v.getTransactions());
      out.write(data);
      out.flush();

      // make sure after the flush the data has been committed
      assertRaftLog(expectedTxs.size(), leader);
    }
    out.close();

    try {
      out.write(0);
      fail("The OutputStream has been closed");
    } catch (IOException ignored) {
    }

    LOG.info("Start to check leader's log");
    final AtomicInteger index = new AtomicInteger(0);
    checkLog(leader.getRaftLog(), expectedTxs.size(),
        () -> expectedTxs.get(index.getAndIncrement()));
  }

  private RaftLog assertRaftLog(int expectedEntries, RaftServer.Division server) throws Exception {
    final RaftLog raftLog = server.getRaftLog();
    final EnumMap<LogEntryBodyCase, AtomicLong> counts = RaftTestUtil.countEntries(raftLog);
    Assert.assertEquals(expectedEntries, counts.get(LogEntryBodyCase.STATEMACHINELOGENTRY).get());

    final LogEntryProto last = RaftTestUtil.getLastEntry(LogEntryBodyCase.STATEMACHINELOGENTRY, raftLog);
    Assert.assertNotNull(last);
    Assert.assertTrue(raftLog.getLastCommittedIndex() >= last.getIndex());
    Assert.assertTrue(server.getInfo().getLastAppliedIndex() >= last.getIndex());
    return raftLog;
  }

  private static class ByteValue {
    final static int BUFFERSIZE = 1024;

    final int length;
    final byte value;
    final int numTx;
    byte[] data;

    ByteValue(int length, byte value) {
      this.length = length;
      this.value = value;
      numTx = (length - 1) / BUFFERSIZE + 1;
    }

    byte[] genData() {
      data = new byte[length];
      Arrays.fill(data, value);
      return data;
    }

    Collection<byte[]> getTransactions() {
      if (data.length <= BUFFERSIZE) {
        return Collections.singletonList(data);
      } else {
        List<byte[]> list = new ArrayList<>();
        for (int i = 0; i < numTx; i++) {
          int txSize = Math.min(BUFFERSIZE, length - BUFFERSIZE * i);
          byte[] t = new byte[txSize];
          Arrays.fill(t, value);
          list.add(t);
        }
        return list;
      }
    }
  }

  @Test
  public void testWriteWithOffset() throws Exception {
    runWithNewCluster(NUM_SERVERS, this::runTestWriteWithOffset);
  }

  private void runTestWriteWithOffset(CLUSTER cluster) throws Exception {
    final int bufferSize = ByteValue.BUFFERSIZE;
    final RaftServer.Division leader = waitForLeader(cluster);

    final OutputStream out = newOutputStream(cluster, bufferSize);

    byte[] b1 = new byte[ByteValue.BUFFERSIZE / 2];
    Arrays.fill(b1, (byte) 1);
    byte[] b2 = new byte[ByteValue.BUFFERSIZE];
    Arrays.fill(b2, (byte) 2);
    byte[] b3 = new byte[ByteValue.BUFFERSIZE * 2 + ByteValue.BUFFERSIZE / 2];
    Arrays.fill(b3, (byte) 3);
    byte[] b4 = new byte[ByteValue.BUFFERSIZE * 4];
    Arrays.fill(b3, (byte) 4);

    byte[] expected = new byte[ByteValue.BUFFERSIZE * 8];
    byte[][] data = new byte[][]{b1, b2, b3, b4};
    final Random random = new Random();
    int totalSize = 0;
    for (byte[] b : data) {
      System.arraycopy(b, 0, expected, totalSize, b.length);
      totalSize += b.length;

      int written = 0;
      while (written < b.length) {
        int toWrite = random.nextInt(b.length - written) + 1;
        LOG.info("write {} bytes", toWrite);
        out.write(b, written, toWrite);
        written += toWrite;
      }
    }
    out.close();

    // 0.5 + 1 + 2.5 + 4 = 8
    final int expectedEntries = 8;
    final RaftLog raftLog = assertRaftLog(expectedEntries, leader);

    final LogEntryHeader[] entries = raftLog.getEntries(1, Long.MAX_VALUE);
    final byte[] actual = new byte[ByteValue.BUFFERSIZE * expectedEntries];
    totalSize = 0;
    for (LogEntryHeader ti : entries) {
      final LogEntryProto e = raftLog.get(ti.getIndex());
      if (e.hasStateMachineLogEntry()) {
        final byte[] eValue = e.getStateMachineLogEntry().getLogData().toByteArray();
        Assert.assertEquals(ByteValue.BUFFERSIZE, eValue.length);
        System.arraycopy(eValue, 0, actual, totalSize, eValue.length);
        totalSize += eValue.length;
      }
    }
    Assert.assertArrayEquals(expected, actual);
  }

  /**
   * Write while leader is killed
   */
  @Test
  public void testKillLeader() throws Exception {
    runWithNewCluster(NUM_SERVERS, this::runTestKillLeader);
  }

  private void runTestKillLeader(CLUSTER cluster) throws Exception {
    final int bufferSize = 4;
    final RaftServer.Division leader = waitForLeader(cluster);

    final AtomicBoolean running  = new AtomicBoolean(true);
    final AtomicReference<Boolean> success = new AtomicReference<>();
    final AtomicInteger result = new AtomicInteger(0);
    final CountDownLatch latch = new CountDownLatch(1);

    new Thread(() -> {
      LOG.info("Writer thread starts");
      int count = 0;
      try (OutputStream out = newOutputStream(cluster, bufferSize)) {
        while (running.get()) {
          out.write(toBytes(count++));
          Thread.sleep(10);
        }
        success.set(true);
        result.set(count);
      } catch (Exception e) {
        LOG.info("Got exception when writing", e);
        success.set(false);
      } finally {
        latch.countDown();
      }
    }).start();

    // force change the leader
    Thread.sleep(500);
    RaftTestUtil.waitAndKillLeader(cluster);
    final RaftServer.Division newLeader = waitForLeader(cluster);
    Assert.assertNotEquals(leader.getId(), newLeader.getId());
    Thread.sleep(500);

    running.set(false);
    latch.await(5, TimeUnit.SECONDS);
    LOG.info("Writer success? " + success.get());
    Assert.assertTrue(success.get());
    // total number of tx should be >= result + 2, where 2 means two NoOp from
    // leaders. It may be larger than result+2 because the client may resend
    // requests and we do not have retry cache on servers yet.
    LOG.info("last applied index: {}. total number of requests: {}",
        newLeader.getInfo().getLastAppliedIndex(), result.get());
    Assert.assertTrue(newLeader.getInfo().getLastAppliedIndex() >= result.get() + 1);
  }
}
