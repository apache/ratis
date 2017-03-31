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
package org.apache.ratis.grpc;

import org.apache.log4j.Level;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.util.LogUtils;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.grpc.client.AppendStreamer;
import org.apache.ratis.grpc.client.RaftOutputStream;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.storage.RaftLog;
import org.apache.ratis.shaded.proto.RaftProtos.LogEntryProto;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.apache.ratis.RaftTestUtil.waitForLeader;
import static org.junit.Assert.fail;

public class TestRaftStream {
  static {
    LogUtils.setLogLevel(AppendStreamer.LOG, Level.ALL);
  }
  static final Logger LOG = LoggerFactory.getLogger(TestRaftStream.class);

  private static final RaftProperties prop = new RaftProperties();
  private static final int NUM_SERVERS = 3;
  private static final byte[] BYTES = new byte[4];

  private MiniRaftClusterWithGRpc cluster;


  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private byte[] toBytes(int i) {
    return toBytes(i, BYTES);
  }
  private byte[] toBytes(int i, byte[] b) {
    b[0] = (byte) ((i >>> 24) & 0xFF);
    b[1] = (byte) ((i >>> 16) & 0xFF);
    b[2] = (byte) ((i >>> 8) & 0xFF);
    b[3] = (byte) (i & 0xFF);
    return b;
  }

  @Test
  public void testSimpleWrite() throws Exception {
    LOG.info("Running testSimpleWrite");

    // default 64K is too large for a test
    GrpcConfigKeys.OutputStream.setBufferSize(prop, SizeInBytes.valueOf(4));
    cluster = MiniRaftClusterWithGRpc.FACTORY.newCluster(NUM_SERVERS, prop);

    cluster.start();
    RaftServerImpl leader = waitForLeader(cluster);

    final Random r = new Random();
    final long seed = r.nextLong();
    r.setSeed(seed);
    try (RaftOutputStream out = new RaftOutputStream(prop, ClientId.createId(),
        cluster.getPeers(), leader.getId())) {
      for (int i = 0; i < 500; i++) { // generate 500 requests
        out.write(toBytes(r.nextInt()));
      }
    }

    // check the leader's raft log
    final RaftLog raftLog = leader.getState().getLog();
    r.setSeed(seed);
    checkLog(raftLog, 500, () -> toBytes(r.nextInt()));
  }

  private void checkLog(RaftLog raftLog, long expectedCommittedIndex,
      Supplier<byte[]> s) {
    long committedIndex = raftLog.getLastCommittedIndex();
    Assert.assertEquals(expectedCommittedIndex, committedIndex);
    // check the log content
    LogEntryProto[] entries = raftLog.getEntries(1, expectedCommittedIndex + 1);
    for (LogEntryProto entry : entries) {
      byte[] logData = entry.getSmLogEntry().getData().toByteArray();
      byte[] expected = s.get();
      Assert.assertEquals("log entry: " + entry,
          expected.length, logData.length);
      Assert.assertArrayEquals(expected, logData);
    }
  }

  @Test
  public void testWriteAndFlush() throws Exception {
    LOG.info("Running testWriteAndFlush");

    GrpcConfigKeys.OutputStream.setBufferSize(prop, SizeInBytes.valueOf(ByteValue.BUFFERSIZE));
    cluster = MiniRaftClusterWithGRpc.FACTORY.newCluster(NUM_SERVERS, prop);
    cluster.start();

    RaftServerImpl leader = waitForLeader(cluster);
    RaftOutputStream out = new RaftOutputStream(prop, ClientId.createId(),
        cluster.getPeers(), leader.getId());

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
      Assert.assertEquals(expectedTxs.size(),
          leader.getState().getLastAppliedIndex());
    }
    out.close();

    try {
      out.write(0);
      fail("The OutputStream has been closed");
    } catch (IOException ignored) {
    }

    LOG.info("Start to check leader's log");
    final AtomicInteger index = new AtomicInteger(0);
    checkLog(leader.getState().getLog(), expectedTxs.size(),
        () -> expectedTxs.get(index.getAndIncrement()));
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
    LOG.info("Running testWriteWithOffset");
    GrpcConfigKeys.OutputStream.setBufferSize(prop, SizeInBytes.valueOf(ByteValue.BUFFERSIZE));

    cluster = MiniRaftClusterWithGRpc.FACTORY.newCluster(NUM_SERVERS, prop);
    cluster.start();
    RaftServerImpl leader = waitForLeader(cluster);

    RaftOutputStream out = new RaftOutputStream(prop, ClientId.createId(),
        cluster.getPeers(), leader.getId());

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

    final RaftLog log = leader.getState().getLog();
    // 0.5 + 1 + 2.5 + 4 = 8
    Assert.assertEquals(8, leader.getState().getLastAppliedIndex());
    Assert.assertEquals(8, log.getLastCommittedIndex());
    LogEntryProto[] entries = log.getEntries(1, 9);
    byte[] actual = new byte[ByteValue.BUFFERSIZE * 8];
    totalSize = 0;
    for (LogEntryProto e : entries) {
      byte[] eValue = e.getSmLogEntry().getData().toByteArray();
      Assert.assertEquals(ByteValue.BUFFERSIZE, eValue.length);
      System.arraycopy(eValue, 0, actual, totalSize, eValue.length);
      totalSize += eValue.length;
    }
    Assert.assertArrayEquals(expected, actual);
  }

  /**
   * Write while leader is killed
   */
  @Test
  public void testKillLeader() throws Exception {
    LOG.info("Running testChangeLeader");

    GrpcConfigKeys.OutputStream.setBufferSize(prop, SizeInBytes.valueOf(4));
    cluster = MiniRaftClusterWithGRpc.FACTORY.newCluster(NUM_SERVERS, prop);
    cluster.start();
    final RaftServerImpl leader = waitForLeader(cluster);

    final AtomicBoolean running  = new AtomicBoolean(true);
    final AtomicBoolean success = new AtomicBoolean(false);
    final AtomicInteger result = new AtomicInteger(0);
    final CountDownLatch latch = new CountDownLatch(1);

    new Thread(() -> {
      LOG.info("Writer thread starts");
      int count = 0;
      try (RaftOutputStream out = new RaftOutputStream(prop, ClientId.createId(),
          cluster.getPeers(), leader.getId())) {
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
    RaftTestUtil.waitAndKillLeader(cluster, true);
    final RaftServerImpl newLeader = waitForLeader(cluster);
    Assert.assertNotEquals(leader.getId(), newLeader.getId());
    Thread.sleep(500);

    running.set(false);
    latch.await(5, TimeUnit.SECONDS);
    Assert.assertTrue(success.get());
    // total number of tx should be >= result + 2, where 2 means two NoOp from
    // leaders. It may be larger than result+2 because the client may resend
    // requests and we do not have retry cache on servers yet.
    LOG.info("last applied index: {}. total number of requests: {}",
        newLeader.getState().getLastAppliedIndex(), result.get());
    Assert.assertTrue(
        newLeader.getState().getLastAppliedIndex() >= result.get() + 1);
  }
}
