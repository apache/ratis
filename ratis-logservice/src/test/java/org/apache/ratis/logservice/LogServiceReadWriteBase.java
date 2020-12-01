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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.Iterator;
import java.util.List;

import javax.management.ObjectName;

import org.apache.ratis.BaseTest;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.logservice.api.LogName;
import org.apache.ratis.logservice.api.LogReader;
import org.apache.ratis.logservice.api.LogStream;
import org.apache.ratis.logservice.api.LogStream.State;
import org.apache.ratis.logservice.api.LogWriter;
import org.apache.ratis.logservice.impl.LogStreamImpl;
import org.apache.ratis.logservice.metrics.LogServiceMetrics;
import org.apache.ratis.logservice.server.LogStateMachine;
import org.apache.ratis.logservice.util.TestUtils;
import org.apache.ratis.metrics.JVMMetrics;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.TimeDuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class LogServiceReadWriteBase<CLUSTER extends MiniRaftCluster>
    extends BaseTest
    implements MiniRaftCluster.Factory.Get<CLUSTER> {
  public static final Logger LOG = LoggerFactory.getLogger(LogServiceReadWriteBase.class);

  static {
    JVMMetrics.initJvmMetrics(TimeDuration.valueOf(10, TimeUnit.SECONDS));
  }

  {
    final RaftProperties p = getProperties();
    p.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        LogStateMachine.class, StateMachine.class);
    LOG.info("Set LogStateMachine OK");
  }

  static final int NUM_PEERS = 3;
  CLUSTER cluster;

  class MetricLogStream extends LogStreamImpl{
    Long startRecordIdCount = 0l;
    Long getStateCount = 0l;
    Long getLastRecordIdCount = 0l;
    Long getLengthCount = 0l;
    Long getSizeCount = 0l;

    public MetricLogStream(LogName name, RaftClient raftClient) {
      super(name, raftClient);
    }

    @Override public long getStartRecordId() throws IOException {
      startRecordIdCount++;
      return super.getStartRecordId();
    }

    @Override public State getState() throws IOException {
      getStateCount++;
      return super.getState();
    }

    @Override public long getLastRecordId() throws IOException {
      getLastRecordIdCount++;
      return super.getLastRecordId();
    }

    @Override public long getLength() throws IOException {
      getLengthCount++;
      return super.getLength();
    }

    @Override public long getSize() throws IOException {
      getSizeCount++;
      return super.getSize();
    }

    @Override public LogName getName() {
      return super.getName();
    }
  }
  @Before
  public void setUpCluster() throws IOException, InterruptedException {
    RaftProperties raftProperties = getProperties();
    cluster = getFactory().newCluster(NUM_PEERS, raftProperties);
    cluster.start();
    RaftTestUtil.waitForLeader(cluster);
  }

  @Test
  public void testLogServiceReadWrite() throws Exception {
    try (RaftClient raftClient =
        RaftClient.newBuilder().setProperties(getProperties())
            .setRaftGroup(cluster.getGroup()).build()) {
      LogName logName = LogName.of("log1");
      // TODO need API to circumvent metadata service for testing
      LogStream logStream = new MetricLogStream(logName, raftClient);
      assertEquals("log1", logStream.getName().getName());
      assertEquals(State.OPEN, logStream.getState());
      assertEquals(0, logStream.getSize());
      assertEquals(0, logStream.getLength());
      testJMXMetrics(logStream);

      LogReader reader = logStream.createReader();
      LogWriter writer = logStream.createWriter();

      // Check last record id
      long lastId = logStream.getLastRecordId();
      LOG.info("last id {}", lastId);

      // Add some records
      List<ByteBuffer> records = TestUtils.getRandomData(100, 10);
      List<Long> ids = writer.write(records);
      LOG.info("ids {}", ids);
      // Check log size and length
      assertEquals(10 * 100, logStream.getSize());
      assertEquals(10, logStream.getLength());

      // Check last record id
      long lastId2 = logStream.getLastRecordId();
      LOG.info("last id {}", lastId2);

      // Check first record id
      long startId = logStream.getStartRecordId();
      LOG.info("start id {}", startId);

      reader.seek(startId);
      // Read records back
      List<ByteBuffer> data = reader.readBulk(records.size());
      assertEquals(records.size(), data.size());

      // Make sure we got the same 10 records that we wrote.
      Iterator<ByteBuffer> expectedIter = records.iterator();
      Iterator<ByteBuffer> actualIter = data.iterator();
      while (expectedIter.hasNext() && actualIter.hasNext()) {
        ByteBuffer expected = expectedIter.next();
        ByteBuffer actual = actualIter.next();
        assertEquals(expected, actual);
      }
      testJMXMetrics(logStream);
      assertEquals(logStream.getState(),State.OPEN);

    }
  }

  @Test
  public void testReadAllRecords() throws Exception {
    try (RaftClient raftClient =
        RaftClient.newBuilder().setProperties(getProperties())
            .setRaftGroup(cluster.getGroup()).build()) {
      final LogName logName = LogName.of("log1");
      final int numRecords = 25;
      // TODO need API to circumvent metadata service for testing
      LogStream logStream = new MetricLogStream(logName, raftClient);
      try (LogWriter writer = logStream.createWriter()) {
        LOG.info("Writing {} records", numRecords);
        // Write records 0 through 99 (inclusive)
        for (int i = 0; i < numRecords; i++) {
          writer.write(toBytes(i));
        }
      }

      try (LogReader reader = logStream.createReader()) {
        reader.seek(0);
        for (int i = 0; i < numRecords; i++) {
          assertEquals(i, fromBytes(reader.readNext()));
        }

        reader.seek(0);
        List<ByteBuffer> records = reader.readBulk(numRecords);
        assertEquals(numRecords, records.size());
        for (int i = 0; i < numRecords; i++) {
          ByteBuffer record = records.get(i);
          assertEquals(i, fromBytes(record));
        }

        reader.seek(0);
        ByteBuffer[] arr = new ByteBuffer[numRecords];
        reader.readBulk(arr);
        for (int i = 0; i < numRecords; i++) {
          assertEquals(i, fromBytes(arr[i]));
        }
      }
    }
  }

  @Test
  public void testSeeking() throws Exception {
    try (final RaftClient raftClient =
        RaftClient.newBuilder().setProperties(getProperties())
            .setRaftGroup(cluster.getGroup()).build()) {
      final LogName logName = LogName.of("log1");
      final int numRecords = 100;
      // TODO need API to circumvent metadata service for testing
      LogStream logStream = new MetricLogStream(logName, raftClient);
      try (LogWriter writer = logStream.createWriter()) {
        LOG.info("Writing {} records", numRecords);
        // Write records 0 through 99 (inclusive)
        for (int i = 0; i < numRecords; i++) {
          writer.write(toBytes(i));
        }
      }

      LOG.debug("Seek and read'ing records");
      try (LogReader reader = logStream.createReader()) {
        for (int i = 9; i < numRecords; i += 10) {
          LOG.info("Seeking to {}", i);
          reader.seek(i);
          LOG.info("Reading one record");
          assertEquals(i, fromBytes(reader.readNext()));
        }

        assertTrue("We're expecting at least two records were written", numRecords > 1);
        for (int i = numRecords - 2; i >= 0; i -= 6) {
          LOG.info("Seeking to {}", i);
          reader.seek(i);
          LOG.info("Reading one record");
          assertEquals(i, fromBytes(reader.readNext()));
        }
      }
    }
  }

  @Test
  public void testSeekFromWrite() throws Exception {
    try (final RaftClient raftClient =
        RaftClient.newBuilder().setProperties(getProperties())
            .setRaftGroup(cluster.getGroup()).build()) {
      final LogName logName = LogName.of("log1");
      final int numRecords = 10;
      LogStream logStream = new MetricLogStream(logName, raftClient);
      final List<Long> recordIds;
      try (LogWriter writer = logStream.createWriter()) {
        LOG.info("Writing {} records", numRecords);
        List<ByteBuffer> records = new ArrayList<>(numRecords * 2);
        // Write records 0 through 10 (inclusive) as one batch
        for (int i = 0; i < numRecords; i++) {
          records.add(toBytes(i));
        }
        recordIds = new ArrayList<>(writer.write(records));
        // Then, write another 10 records, individually.
        for (int i = numRecords; i < numRecords*2; i++) {
          recordIds.add(writer.write(toBytes(i)));
        }
      }

      // We should have numRecords recordIds
      assertEquals(numRecords * 2, recordIds.size());
      // We should have monotonically increasing recordIds because we're the only one
      // writing to this log.
      assertEquals(LongStream.range(0, numRecords * 2).boxed().collect(Collectors.toList()),
          recordIds);

      try (LogReader reader = logStream.createReader()) {
        int i = 0;
        // We should be able to seek to the recordId given for each record
        // we wrote and read it back.
        for (long recordId : recordIds) {
          reader.seek(recordId);
          int readValue = fromBytes(reader.readNext());
          assertEquals("Seeked to " + recordId + " but got " + readValue, i++, readValue);
        }
      }
    }
  }

  @After
  public void tearDown() {
    cluster.shutdown();
  }

  private ByteBuffer toBytes(int i) {
    return ByteBuffer.wrap(Integer.toString(i).getBytes(StandardCharsets.UTF_8));
  }

  private int fromBytes(ByteBuffer bb) {
    byte[] bytes = new byte[bb.remaining()];
    System.arraycopy(bb.array(), bb.arrayOffset(), bytes, 0, bb.remaining());
    return Integer.parseInt(new String(bytes, StandardCharsets.UTF_8));
  }

  private void testJMXMetrics(LogStream logStream) throws Exception {
    assertEquals(((MetricLogStream) logStream).getLengthCount,
        getJMXCount(cluster.getGroup().getGroupId().toString(), "lengthQueryTime"));
    assertEquals(((MetricLogStream) logStream).getSizeCount,
        getJMXCount(cluster.getGroup().getGroupId().toString(), "sizeRequestTime"));

  }

  private Long getJMXCount(String groupId, String metricName) throws Exception {
    ObjectName oname = new ObjectName(LogServiceMetrics.RATIS_LOG_SERVICE_METRICS, "name",
        new LogServiceMetrics(groupId, cluster.getLeader().getId().toString())
            .getRegistry().getMetricRegistryInfo().getName() + "." + metricName);
    return (Long) ManagementFactory.getPlatformMBeanServer().getAttribute(oname, "Count");
  }
}
