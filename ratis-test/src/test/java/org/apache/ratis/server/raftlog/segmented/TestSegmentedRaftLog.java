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
package org.apache.ratis.server.raftlog.segmented;

import org.apache.ratis.BaseTest;
import org.apache.ratis.RaftTestUtil.SimpleOperation;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.metrics.RatisMetricRegistry;
import org.apache.ratis.metrics.impl.DefaultTimekeeperImpl;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.TimeoutIOException;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.RetryCacheTestUtil;
import org.apache.ratis.server.RetryCache;
import org.apache.ratis.server.metrics.RaftLogMetricsBase;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.server.raftlog.LogEntryHeader;
import org.apache.ratis.server.raftlog.LogProtoUtils;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.server.storage.RaftStorageTestUtils;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.SimpleStateMachine4Testing;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.util.CodeInjectionForTesting;
import org.apache.ratis.util.DataBlockingQueue;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.ReferenceCountedObject;
import org.apache.ratis.util.Slf4jUtils;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.event.Level;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static org.apache.ratis.server.raftlog.segmented.SegmentedRaftLogWorker.RUN_WORKER;
import static org.apache.ratis.server.storage.RaftStorageTestUtils.getLogUnsafe;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class TestSegmentedRaftLog extends BaseTest {
  static {
    Slf4jUtils.setLogLevel(SegmentedRaftLogWorker.LOG, Level.INFO);
    Slf4jUtils.setLogLevel(SegmentedRaftLogCache.LOG, Level.INFO);
    Slf4jUtils.setLogLevel(SegmentedRaftLog.LOG, Level.INFO);
  }

  public static Stream<Arguments> data() {
    return Stream.of(
        arguments(FALSE, FALSE),
        arguments(FALSE, TRUE),
        arguments(TRUE, FALSE),
        arguments(TRUE, TRUE));
  }

  public static long getOpenSegmentSize(RaftLog raftLog) {
    return ((SegmentedRaftLog)raftLog).getRaftLogCache().getOpenSegment().getTotalFileSize();
  }

  private static final RaftPeerId PEER_ID = RaftPeerId.valueOf("s0");
  private static final RaftGroupId GROUP_ID = RaftGroupId.randomId();
  private static final RaftGroupMemberId MEMBER_ID = RaftGroupMemberId.valueOf(PEER_ID, GROUP_ID);

  static class SegmentRange {
    final long start;
    final long end;
    final long term;
    final boolean isOpen;

    SegmentRange(long s, long e, long term, boolean isOpen) {
      this.start = s;
      this.end = e;
      this.term = term;
      this.isOpen = isOpen;
    }

    File getFile(RaftStorage storage) {
      return LogSegmentStartEnd.valueOf(start, end, isOpen).getFile(storage);
    }
  }

  private File storageDir;
  private RaftProperties properties;
  private RaftStorage storage;
  private long segmentMaxSize;
  private long preallocatedSize;
  private int bufferSize;

  SegmentedRaftLog newSegmentedRaftLog() {
    return newSegmentedRaftLog(storage, properties);
  }

  SegmentedRaftLog newSegmentedRaftLog(LongSupplier getSnapshotIndexFromStateMachine) {
    return newSegmentedRaftLogWithSnapshotIndex(storage, properties, getSnapshotIndexFromStateMachine);
  }

  static SegmentedRaftLog newSegmentedRaftLog(RaftStorage storage, RaftProperties properties) {
    return SegmentedRaftLog.newBuilder()
        .setMemberId(MEMBER_ID)
        .setStorage(storage)
        .setProperties(properties)
        .build();
  }

  private SegmentedRaftLog newSegmentedRaftLogWithSnapshotIndex(RaftStorage storage, RaftProperties properties,
                                                                LongSupplier getSnapshotIndexFromStateMachine) {
    return SegmentedRaftLog.newBuilder()
        .setMemberId(MEMBER_ID)
        .setStorage(storage)
        .setSnapshotIndexSupplier(getSnapshotIndexFromStateMachine)
        .setProperties(properties)
        .build();
  }

  @BeforeEach
  public void setup() throws Exception {
    storageDir = getTestDir();
    properties = new RaftProperties();
    RaftServerConfigKeys.setStorageDir(properties,  Collections.singletonList(storageDir));
    storage = RaftStorageTestUtils.newRaftStorage(storageDir);
    this.segmentMaxSize =
        RaftServerConfigKeys.Log.segmentSizeMax(properties).getSize();
    this.preallocatedSize =
        RaftServerConfigKeys.Log.preallocatedSize(properties).getSize();
    this.bufferSize =
        RaftServerConfigKeys.Log.writeBufferSize(properties).getSizeInt();
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (storageDir != null) {
      FileUtils.deleteFully(storageDir.getParentFile());
    }
  }

  private LogEntryProto[] prepareLog(List<SegmentRange> list) throws IOException {
    List<LogEntryProto> entryList = new ArrayList<>();
    for (SegmentRange range : list) {
      final File file = range.getFile(storage);
      final int size = (int) (range.end - range.start + 1);
      LogEntryProto[] entries = new LogEntryProto[size];
      try (SegmentedRaftLogOutputStream out = new SegmentedRaftLogOutputStream(file, false,
          segmentMaxSize, preallocatedSize, ByteBuffer.allocateDirect(bufferSize))) {
        for (int i = 0; i < size; i++) {
          SimpleOperation m = new SimpleOperation("m" + (i + range.start));
          entries[i] = LogProtoUtils.toLogEntryProto(m.getLogEntryContent(), range.term, i + range.start);
          out.write(entries[i]);
        }
      }
      Collections.addAll(entryList, entries);
    }
    return entryList.toArray(new LogEntryProto[entryList.size()]);
  }

  static List<SegmentRange> prepareRanges(int startTerm, int endTerm, int segmentSize,
      long startIndex) {
    List<SegmentRange> list = new ArrayList<>(endTerm - startTerm);
    for (int i = startTerm; i < endTerm; i++) {
      list.add(new SegmentRange(startIndex, startIndex + segmentSize - 1, i,
          i == endTerm - 1));
      startIndex += segmentSize;
    }
    return list;
  }

  private LogEntryProto getLastEntry(SegmentedRaftLog raftLog)
      throws IOException {
    return getLogUnsafe(raftLog, raftLog.getLastEntryTermIndex().getIndex());
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testLoadLogSegments(Boolean useAsyncFlush, Boolean smSyncFlush) throws Exception {
    RaftServerConfigKeys.Log.setAsyncFlushEnabled(properties, useAsyncFlush);
    RaftServerConfigKeys.Log.StateMachineData.setSync(properties, smSyncFlush);
    // first generate log files
    List<SegmentRange> ranges = prepareRanges(0, 5, 100, 0);
    LogEntryProto[] entries = prepareLog(ranges);

    // create RaftLog object and load log file
    try (SegmentedRaftLog raftLog = newSegmentedRaftLog()) {
      raftLog.open(RaftLog.INVALID_LOG_INDEX, null);
      // check if log entries are loaded correctly
      for (LogEntryProto e : entries) {
        LogEntryProto entry = raftLog.get(e.getIndex());
        Assertions.assertEquals(e, entry);
      }

      final LogEntryHeader[] termIndices = raftLog.getEntries(0, 500);
      LogEntryProto[] entriesFromLog = Arrays.stream(termIndices)
          .map(ti -> {
            try {
              return getLogUnsafe(raftLog, ti.getIndex());
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          })
          .toArray(LogEntryProto[]::new);
      Assertions.assertArrayEquals(entries, entriesFromLog);
      Assertions.assertEquals(entries[entries.length - 1], getLastEntry(raftLog));

      final RatisMetricRegistry metricRegistryForLogWorker = RaftLogMetricsBase.createRegistry(MEMBER_ID);

      final DefaultTimekeeperImpl load = (DefaultTimekeeperImpl) metricRegistryForLogWorker.timer("segmentLoadLatency");
      assertTrue(load.getTimer().getMeanRate() > 0);

      final DefaultTimekeeperImpl read = (DefaultTimekeeperImpl) metricRegistryForLogWorker.timer("readEntryLatency");
      assertTrue(read.getTimer().getMeanRate() > 0);
    }
  }

  static List<LogEntryProto> prepareLogEntries(List<SegmentRange> slist,
      Supplier<String> stringSupplier) {
    List<LogEntryProto> eList = new ArrayList<>();
    for (SegmentRange range : slist) {
      prepareLogEntries(range, stringSupplier, false, eList);
    }
    return eList;
  }

  static List<LogEntryProto> prepareLogEntries(SegmentRange range,
      Supplier<String> stringSupplier, boolean hasStataMachineData, List<LogEntryProto> eList) {
    for(long index = range.start; index <= range.end; index++) {
      eList.add(prepareLogEntry(range.term, index, stringSupplier, hasStataMachineData));
    }
    return eList;
  }

  static LogEntryProto prepareLogEntry(long term, long index, Supplier<String> stringSupplier,
      boolean hasStataMachineData) {
    final SimpleOperation m = stringSupplier == null?
        new SimpleOperation("m" + index, hasStataMachineData):
        new SimpleOperation(stringSupplier.get(), hasStataMachineData);
    return LogProtoUtils.toLogEntryProto(m.getLogEntryContent(), term, index);
  }

  /**
   * Append entry one by one and check if log state is correct.
   */
  @ParameterizedTest
  @MethodSource("data")
  public void testAppendEntry(Boolean useAsyncFlush, Boolean smSyncFlush) throws Exception {
    RaftServerConfigKeys.Log.setAsyncFlushEnabled(properties, useAsyncFlush);
    RaftServerConfigKeys.Log.StateMachineData.setSync(properties, smSyncFlush);
    List<SegmentRange> ranges = prepareRanges(0, 5, 200, 0);
    List<LogEntryProto> entries = prepareLogEntries(ranges, null);

    try (SegmentedRaftLog raftLog = newSegmentedRaftLog()) {
      raftLog.open(RaftLog.INVALID_LOG_INDEX, null);
      // append entries to the raftlog
      entries.stream().map(raftLog::appendEntry).forEach(CompletableFuture::join);
    }

    try (SegmentedRaftLog raftLog = newSegmentedRaftLog()) {
      raftLog.open(RaftLog.INVALID_LOG_INDEX, null);
      // check if the raft log is correct
      checkEntries(raftLog, entries, 0, entries.size());
    }

    try (SegmentedRaftLog raftLog = newSegmentedRaftLog()) {
      raftLog.open(RaftLog.INVALID_LOG_INDEX, null);
      TermIndex lastTermIndex  = raftLog.getLastEntryTermIndex();
      IllegalStateException ex = null;
      try {
        // append entry fails if append entry term is lower than log's last entry term
        raftLog.appendEntry(LogEntryProto.newBuilder(entries.get(0))
            .setTerm(lastTermIndex.getTerm() - 1)
            .setIndex(lastTermIndex.getIndex() + 1).build());
      } catch (IllegalStateException e) {
        ex = e;
      }
      assertTrue(ex.getMessage().contains("term less than RaftLog's last term"));
      try {
        // append entry fails if difference between append entry index and log's last entry index is greater than 1
        raftLog.appendEntry(LogEntryProto.newBuilder(entries.get(0))
            .setTerm(lastTermIndex.getTerm())
            .setIndex(lastTermIndex.getIndex() + 2).build());
      } catch (IllegalStateException e) {
        ex = e;
      }
      assertTrue(ex.getMessage().contains("and RaftLog's last index " + lastTermIndex.getIndex()
          + " (or snapshot index " + raftLog.getSnapshotIndex() + ") is greater than 1"));

      raftLog.onSnapshotInstalled(raftLog.getLastEntryTermIndex().getIndex());
      try {
        // append entry fails if there are no log entries && log's snapshotIndex + 1 < incoming log entry.
        raftLog.appendEntry(LogEntryProto.newBuilder(entries.get(0))
            .setTerm(lastTermIndex.getTerm())
            .setIndex(lastTermIndex.getIndex() + 2).build());
      } catch (IllegalStateException e) {
        ex = e;
      }
      assertTrue(ex.getMessage().contains("Difference between entry index and RaftLog's latest snapshot " +
          "index 999 is greater than 1"));
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testAppendEntryAfterPurge(Boolean useAsyncFlush, Boolean smSyncFlush) throws Exception {
    RaftServerConfigKeys.Log.setAsyncFlushEnabled(properties, useAsyncFlush);
    RaftServerConfigKeys.Log.StateMachineData.setSync(properties, smSyncFlush);
    List<SegmentRange> ranges = prepareRanges(0, 5, 200, 0);
    List<LogEntryProto> entries = prepareLogEntries(ranges, null);

    long desiredSnapshotIndex = entries.size() - 2;
    final LongSupplier getSnapshotIndexFromStateMachine = new LongSupplier() {
      private boolean firstCall = true;
      @Override
      public long getAsLong() {
        long index = firstCall ? -1 : desiredSnapshotIndex;
        firstCall = !firstCall;
        return index;
      }
    };

    try (SegmentedRaftLog raftLog = newSegmentedRaftLog(getSnapshotIndexFromStateMachine)) {
      raftLog.open(RaftLog.INVALID_LOG_INDEX, null);
      entries.subList(0, entries.size() - 1).stream().map(raftLog::appendEntry).forEach(CompletableFuture::join);

      raftLog.onSnapshotInstalled(desiredSnapshotIndex);
      // Try appending last entry after snapshot + purge.
      CompletableFuture<Long> appendEntryFuture =
          raftLog.appendEntry(entries.get(entries.size() - 1));
      assertTrue(desiredSnapshotIndex + 1 == appendEntryFuture.get());
    }
  }

  /**
   * Keep appending entries, make sure the rolling is correct.
   */
  @ParameterizedTest
  @MethodSource("data")
  public void testAppendAndRoll(Boolean useAsyncFlush, Boolean smSyncFlush) throws Exception {
    RaftServerConfigKeys.Log.setAsyncFlushEnabled(properties, useAsyncFlush);
    RaftServerConfigKeys.Log.StateMachineData.setSync(properties, smSyncFlush);
    RaftServerConfigKeys.Log.setPreallocatedSize(properties, SizeInBytes.valueOf("16KB"));
    RaftServerConfigKeys.Log.setSegmentSizeMax(properties, SizeInBytes.valueOf("128KB"));

    List<SegmentRange> ranges = prepareRanges(0, 1, 1024, 0);
    final byte[] content = new byte[1024];
    List<LogEntryProto> entries = prepareLogEntries(ranges,
        () -> new String(content));

    try (SegmentedRaftLog raftLog = newSegmentedRaftLog()) {
      raftLog.open(RaftLog.INVALID_LOG_INDEX, null);
      // append entries to the raftlog
      entries.stream().map(raftLog::appendEntry).forEach(CompletableFuture::join);
    }

    try (SegmentedRaftLog raftLog = newSegmentedRaftLog()) {
      raftLog.open(RaftLog.INVALID_LOG_INDEX, null);
      // check if the raft log is correct
      checkEntries(raftLog, entries, 0, entries.size());
      Assertions.assertEquals(9, raftLog.getRaftLogCache().getNumOfSegments());
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testPurgeAfterAppendEntry(Boolean useAsyncFlush, Boolean smSyncFlush) throws Exception {
    RaftServerConfigKeys.Log.setAsyncFlushEnabled(properties, useAsyncFlush);
    RaftServerConfigKeys.Log.StateMachineData.setSync(properties, smSyncFlush);
    RaftServerConfigKeys.Log.setPurgeGap(properties, 1);
    RaftServerConfigKeys.Log.setForceSyncNum(properties, 128);

    int startTerm = 0;
    int endTerm = 2;
    int segmentSize = 10;
    long endIndexOfClosedSegment = segmentSize * (endTerm - startTerm - 1);
    long nextStartIndex = segmentSize * (endTerm - startTerm);

    // append entries and roll logSegment for later purge operation
    List<SegmentRange> ranges0 = prepareRanges(startTerm, endTerm, segmentSize, 0);
    List<LogEntryProto> entries0 = prepareLogEntries(ranges0, null);
    try (SegmentedRaftLog raftLog = newSegmentedRaftLog()) {
      raftLog.open(RaftLog.INVALID_LOG_INDEX, null);
      entries0.stream().map(raftLog::appendEntry).forEach(CompletableFuture::join);
    }

    // test the pattern in the task queue of SegmentedRaftLogWorker: (WriteLog, ..., PurgeLog)
    List<SegmentRange> ranges = prepareRanges(endTerm - 1, endTerm, 1, nextStartIndex);
    List<LogEntryProto> entries = prepareLogEntries(ranges, null);

    try (SegmentedRaftLog raftLog = newSegmentedRaftLog()) {
      final CountDownLatch raftLogOpened = new CountDownLatch(1);
      final CountDownLatch tasksAdded = new CountDownLatch(1);

      // inject test code to make the pattern (WriteLog, PurgeLog)
      final ConcurrentLinkedQueue<CompletableFuture<Long>> appendFutures = new ConcurrentLinkedQueue<>();
      final AtomicReference<CompletableFuture<Long>> purgeFuture = new AtomicReference<>();
      final AtomicInteger tasksCount = new AtomicInteger(0);
      CodeInjectionForTesting.put(RUN_WORKER, (localId, remoteId, args) -> {
        // wait for raftLog to be opened
        try {
          if(!raftLogOpened.await(FIVE_SECONDS.getDuration(), FIVE_SECONDS.getUnit())) {
            throw new TimeoutException();
          }
        } catch (InterruptedException | TimeoutException e) {
          LOG.error("an exception occurred", e);
          throw new RuntimeException(e);
        }

        // add WriteLog and PurgeLog tasks
        entries.stream().map(raftLog::appendEntry).forEach(appendFutures::add);
        purgeFuture.set(raftLog.purge(endIndexOfClosedSegment));

        tasksCount.set(((DataBlockingQueue<?>) args[0]).getNumElements());
        tasksAdded.countDown();
        return true;
      });

      // open raftLog
      raftLog.open(RaftLog.INVALID_LOG_INDEX, null);
      raftLogOpened.countDown();

      // wait for all tasks to be added
      if(!tasksAdded.await(FIVE_SECONDS.getDuration(), FIVE_SECONDS.getUnit())) {
        throw new TimeoutException();
      }
      Assertions.assertEquals(entries.size() + 1, tasksCount.get());

      // check if the purge task is executed
      final Long purged = purgeFuture.get().get();
      LOG.info("purgeIndex = {}, purged = {}", endIndexOfClosedSegment, purged);
      Assertions.assertEquals(endIndexOfClosedSegment, raftLog.getRaftLogCache().getStartIndex());

      // check if the appendEntry futures are done
      JavaUtils.allOf(appendFutures).get(FIVE_SECONDS.getDuration(), FIVE_SECONDS.getUnit());
    } finally {
      CodeInjectionForTesting.put(RUN_WORKER, (localId, remoteId, args) -> false);
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testTruncate(Boolean useAsyncFlush, Boolean smSyncFlush) throws Exception {
    RaftServerConfigKeys.Log.setAsyncFlushEnabled(properties, useAsyncFlush);
    RaftServerConfigKeys.Log.StateMachineData.setSync(properties, smSyncFlush);
    // prepare the log for truncation
    List<SegmentRange> ranges = prepareRanges(0, 5, 200, 0);
    List<LogEntryProto> entries = prepareLogEntries(ranges, null);

    try (SegmentedRaftLog raftLog = newSegmentedRaftLog()) {
      raftLog.open(RaftLog.INVALID_LOG_INDEX, null);
      // append entries to the raftlog
      entries.stream().map(raftLog::appendEntry).forEach(CompletableFuture::join);
    }

    for (long fromIndex = 900; fromIndex >= 0; fromIndex -= 150) {
      testTruncate(entries, fromIndex);
    }
  }

  private void testTruncate(List<LogEntryProto> entries, long fromIndex)
      throws Exception {
    try (SegmentedRaftLog raftLog = newSegmentedRaftLog()) {
      raftLog.open(RaftLog.INVALID_LOG_INDEX, null);
      // truncate the log
      raftLog.truncate(fromIndex).join();


      checkEntries(raftLog, entries, 0, (int) fromIndex);
    }

    try (SegmentedRaftLog raftLog = newSegmentedRaftLog()) {
      raftLog.open(RaftLog.INVALID_LOG_INDEX, null);
      // check if the raft log is correct
      if (fromIndex > 0) {
        Assertions.assertEquals(entries.get((int) (fromIndex - 1)),
            getLastEntry(raftLog));
      } else {
        Assertions.assertNull(raftLog.getLastEntryTermIndex());
      }
      checkEntries(raftLog, entries, 0, (int) fromIndex);
    }
  }

  private void checkEntries(RaftLog raftLog, List<LogEntryProto> expected,
      int offset, int size) throws IOException {
    if (size > 0) {
      for (int i = offset; i < size + offset; i++) {
        LogEntryProto entry = raftLog.get(expected.get(i).getIndex());
        Assertions.assertEquals(expected.get(i), entry);
      }
      final LogEntryHeader[] termIndices = raftLog.getEntries(
          expected.get(offset).getIndex(),
          expected.get(offset + size - 1).getIndex() + 1);
      LogEntryProto[] entriesFromLog = Arrays.stream(termIndices)
          .map(ti -> {
            try {
              return getLogUnsafe(raftLog, ti.getIndex());
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          })
          .toArray(LogEntryProto[]::new);
      LogEntryProto[] expectedArray = expected.subList(offset, offset + size)
          .stream().toArray(LogEntryProto[]::new);
      Assertions.assertArrayEquals(expectedArray, entriesFromLog);
    }
  }

  private void checkFailedEntries(List<LogEntryProto> entries, long fromIndex, RetryCache retryCache) {
    for (int i = 0; i < entries.size(); i++) {
      if (i < fromIndex) {
        RetryCacheTestUtil.assertFailure(retryCache, entries.get(i), false);
      } else {
        RetryCacheTestUtil.assertFailure(retryCache, entries.get(i), true);
      }
    }
  }

  @Test
  public void testPurgeOnOpenSegment() throws Exception {
    int startTerm = 0;
    int endTerm = 5;
    int segmentSize = 200;
    long beginIndexOfOpenSegment = segmentSize * (endTerm - startTerm - 1);
    long expectedIndex = segmentSize * (endTerm - startTerm - 1);
    purgeAndVerify(startTerm, endTerm, segmentSize, 1, beginIndexOfOpenSegment, expectedIndex);
  }

  @Test
  public void testPurgeOnClosedSegments() throws Exception {
    int startTerm = 0;
    int endTerm = 5;
    int segmentSize = 200;
    long endIndexOfClosedSegment = segmentSize * (endTerm - startTerm - 1) - 1;
    long expectedIndex = segmentSize * (endTerm - startTerm - 1);
    purgeAndVerify(startTerm, endTerm, segmentSize, 1, endIndexOfClosedSegment, expectedIndex);
  }

  @Test
  public void testPurgeLogMetric() throws Exception {
    int startTerm = 0;
    int endTerm = 5;
    int segmentSize = 200;
    long endIndexOfClosedSegment = segmentSize * (endTerm - startTerm - 1) - 1;
    long expectedIndex = segmentSize * (endTerm - startTerm - 1);
    final RatisMetricRegistry metricRegistryForLogWorker = RaftLogMetricsBase.createRegistry(MEMBER_ID);
    purgeAndVerify(startTerm, endTerm, segmentSize, 1, endIndexOfClosedSegment, expectedIndex);
    final DefaultTimekeeperImpl purge = (DefaultTimekeeperImpl) metricRegistryForLogWorker.timer("purgeLog");
    assertTrue(purge.getTimer().getCount() > 0);
  }

  @Test
  public void testPurgeOnClosedSegmentsWithPurgeGap() throws Exception {
    int startTerm = 0;
    int endTerm = 5;
    int segmentSize = 200;
    long endIndexOfClosedSegment = segmentSize * (endTerm - startTerm - 1) - 1;
    long expectedIndex = RaftLog.LEAST_VALID_LOG_INDEX;
    purgeAndVerify(startTerm, endTerm, segmentSize, 1000, endIndexOfClosedSegment, expectedIndex);
  }

  private void purgeAndVerify(int startTerm, int endTerm, int segmentSize, int purgeGap, long purgeIndex,
      long expectedIndex) throws Exception {
    List<SegmentRange> ranges = prepareRanges(startTerm, endTerm, segmentSize, 0);
    List<LogEntryProto> entries = prepareLogEntries(ranges, null);

    final RaftProperties p = new RaftProperties();
    RaftServerConfigKeys.Log.setPurgeGap(p, purgeGap);
    try (SegmentedRaftLog raftLog = newSegmentedRaftLog(storage, p)) {
      raftLog.open(RaftLog.INVALID_LOG_INDEX, null);
      entries.stream().map(raftLog::appendEntry).forEach(CompletableFuture::join);
      final CompletableFuture<Long> f = raftLog.purge(purgeIndex);
      final Long purged = f.get();
      LOG.info("purgeIndex = {}, purged = {}", purgeIndex, purged);
      Assertions.assertEquals(expectedIndex, raftLog.getRaftLogCache().getStartIndex());
    }
  }

  /**
   * Test append with inconsistent entries
   */
  @ParameterizedTest
  @MethodSource("data")
  public void testAppendEntriesWithInconsistency(Boolean useAsyncFlush, Boolean smSyncFlush) throws Exception {
    RaftServerConfigKeys.Log.setAsyncFlushEnabled(properties, useAsyncFlush);
    RaftServerConfigKeys.Log.StateMachineData.setSync(properties, smSyncFlush);
    // prepare the log for truncation
    List<SegmentRange> ranges = prepareRanges(0, 5, 200, 0);
    List<LogEntryProto> entries = prepareLogEntries(ranges, null);

    final RetryCache retryCache = RetryCacheTestUtil.createRetryCache();
    try (SegmentedRaftLog raftLog =
             RetryCacheTestUtil.newSegmentedRaftLog(MEMBER_ID, retryCache, storage, properties)) {
      raftLog.open(RaftLog.INVALID_LOG_INDEX, null);
      entries.forEach(entry -> RetryCacheTestUtil.createEntry(retryCache, entry));
      // append entries to the raftlog
      entries.stream().map(raftLog::appendEntry).forEach(CompletableFuture::join);
    }

    // append entries whose first 100 entries are the same with existing log,
    // and the next 100 are with different term
    SegmentRange r1 = new SegmentRange(550, 599, 2, false);
    SegmentRange r2 = new SegmentRange(600, 649, 3, false);
    SegmentRange r3 = new SegmentRange(650, 749, 10, false);
    List<LogEntryProto> newEntries = prepareLogEntries(
        Arrays.asList(r1, r2, r3), null);

    try (SegmentedRaftLog raftLog =
             RetryCacheTestUtil.newSegmentedRaftLog(MEMBER_ID, retryCache, storage, properties)) {
      raftLog.open(RaftLog.INVALID_LOG_INDEX, null);
      LOG.info("newEntries[0] = {}", newEntries.get(0));
      final int last = newEntries.size() - 1;
      LOG.info("newEntries[{}] = {}", last, newEntries.get(last));
      raftLog.append(ReferenceCountedObject.wrap(newEntries)).forEach(CompletableFuture::join);

      checkFailedEntries(entries, 650, retryCache);
      checkEntries(raftLog, entries, 0, 650);
      checkEntries(raftLog, newEntries, 100, 100);
      Assertions.assertEquals(newEntries.get(newEntries.size() - 1),
          getLastEntry(raftLog));
      Assertions.assertEquals(newEntries.get(newEntries.size() - 1).getIndex(),
          raftLog.getFlushIndex());
    }

    // load the raftlog again and check
    try (SegmentedRaftLog raftLog =
             RetryCacheTestUtil.newSegmentedRaftLog(MEMBER_ID, retryCache, storage, properties)) {
      raftLog.open(RaftLog.INVALID_LOG_INDEX, null);
      checkEntries(raftLog, entries, 0, 650);
      checkEntries(raftLog, newEntries, 100, 100);
      Assertions.assertEquals(newEntries.get(newEntries.size() - 1),
          getLastEntry(raftLog));
      Assertions.assertEquals(newEntries.get(newEntries.size() - 1).getIndex(),
          raftLog.getFlushIndex());

      SegmentedRaftLogCache cache = raftLog.getRaftLogCache();
      Assertions.assertEquals(5, cache.getNumOfSegments());
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testSegmentedRaftLogStateMachineData(Boolean useAsyncFlush, Boolean smSyncFlush) throws Exception {
    RaftServerConfigKeys.Log.setAsyncFlushEnabled(properties, useAsyncFlush);
    RaftServerConfigKeys.Log.StateMachineData.setSync(properties, smSyncFlush);
    final SegmentRange range = new SegmentRange(0, 10, 1, true);
    final List<LogEntryProto> entries = prepareLogEntries(range, null, true, new ArrayList<>());

    final SimpleStateMachine4Testing sm = new SimpleStateMachine4Testing();
    try (SegmentedRaftLog raftLog = SegmentedRaftLog.newBuilder()
        .setMemberId(MEMBER_ID)
        .setStateMachine(sm)
        .setStorage(storage)
        .setProperties(properties)
        .build()) {
      raftLog.open(RaftLog.INVALID_LOG_INDEX, null);

      int next = 0;
      long flush = -1;
      assertIndices(raftLog, flush, next);
      raftLog.appendEntry(entries.get(next++));
      assertIndices(raftLog, flush, next);
      raftLog.appendEntry(entries.get(next++));
      assertIndices(raftLog, flush, next);
      raftLog.appendEntry(entries.get(next++));
      assertIndicesMultipleAttempts(raftLog, flush += 3, next);

      sm.blockFlushStateMachineData();
      raftLog.appendEntry(entries.get(next++));

      sm.blockWriteStateMachineData();
      final Thread t = startAppendEntryThread(raftLog, entries.get(next++));
      TimeUnit.SECONDS.sleep(1);
      assertTrue(t.isAlive());
      sm.unblockWriteStateMachineData();

      assertIndices(raftLog, flush, next);
      TimeUnit.SECONDS.sleep(1);
      assertIndices(raftLog, flush, next);
      sm.unblockFlushStateMachineData();
      assertIndicesMultipleAttempts(raftLog, flush + 2, next);

      // raftLog.appendEntry(entry).get() won't return
      // until sm.unblockFlushStateMachineData() was called.
      t.join();
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testServerShutdownOnTimeoutIOException(Boolean useAsyncFlush, Boolean smSyncFlush) throws Throwable {
    RaftServerConfigKeys.Log.setAsyncFlushEnabled(properties, useAsyncFlush);
    RaftServerConfigKeys.Log.StateMachineData.setSync(properties, smSyncFlush);
    RaftServerConfigKeys.Log.StateMachineData.setSync(properties, true);
    final TimeDuration syncTimeout = TimeDuration.valueOf(100, TimeUnit.MILLISECONDS);
    RaftServerConfigKeys.Log.StateMachineData.setSyncTimeout(properties, syncTimeout);
    final int numRetries = 2;
    RaftServerConfigKeys.Log.StateMachineData.setSyncTimeoutRetry(properties, numRetries);

    final LogEntryProto entry = prepareLogEntry(0, 0, null, true);
    final StateMachine sm = new BaseStateMachine() {
      @Override
      public CompletableFuture<Void> write(ReferenceCountedObject<LogEntryProto> entry, TransactionContext context) {
        getLifeCycle().transition(LifeCycle.State.STARTING);
        getLifeCycle().transition(LifeCycle.State.RUNNING);

        return new CompletableFuture<>(); // the future never completes
      }

      @Override
      public void notifyLogFailed(Throwable cause, LogEntryProto entry) {
        LOG.info("Test StateMachine: Ratis log failed notification received as expected.", cause);

        LOG.info("Test StateMachine: Transition to PAUSED state.");
        Assertions.assertNotNull(entry);

        getLifeCycle().transition(LifeCycle.State.PAUSING);
        getLifeCycle().transition(LifeCycle.State.PAUSED);
      }
    };

    ExecutionException ex;
    try (SegmentedRaftLog raftLog = SegmentedRaftLog.newBuilder()
        .setMemberId(MEMBER_ID)
        .setStateMachine(sm)
        .setStorage(storage)
        .setProperties(properties)
        .build()) {
      raftLog.open(RaftLog.INVALID_LOG_INDEX, null);
      // SegmentedRaftLogWorker should catch TimeoutIOException
      CompletableFuture<Long> f = raftLog.appendEntry(entry);
      // Wait for async writeStateMachineData to finish
      ex = Assertions.assertThrows(ExecutionException.class, f::get);
    }
    Assertions.assertSame(LifeCycle.State.PAUSED, sm.getLifeCycleState());
    Assertions.assertInstanceOf(TimeoutIOException.class, ex.getCause());
  }

  static Thread startAppendEntryThread(RaftLog raftLog, LogEntryProto entry) {
    final Thread t = new Thread(() -> {
      try {
        raftLog.appendEntry(entry).get();
      } catch (Throwable e) {
        // just ignore
      }
    });
    t.start();
    return t;
  }

  void assertIndices(RaftLog raftLog, long expectedFlushIndex, long expectedNextIndex) {
    LOG.info("assert expectedFlushIndex={}", expectedFlushIndex);
    Assertions.assertEquals(expectedFlushIndex, raftLog.getFlushIndex());
    LOG.info("assert expectedNextIndex={}", expectedNextIndex);
    Assertions.assertEquals(expectedNextIndex, raftLog.getNextIndex());
  }

  void assertIndicesMultipleAttempts(RaftLog raftLog, long expectedFlushIndex, long expectedNextIndex)
      throws Exception {
    JavaUtils.attempt(() -> assertIndices(raftLog, expectedFlushIndex, expectedNextIndex),
        10, HUNDRED_MILLIS, "assertIndices", LOG);
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testAsyncFlushPerf1(Boolean useAsyncFlush, Boolean smSyncFlush) throws Exception {
    RaftServerConfigKeys.Log.setAsyncFlushEnabled(properties, useAsyncFlush);
    RaftServerConfigKeys.Log.StateMachineData.setSync(properties, smSyncFlush);
    List<SegmentRange> ranges = prepareRanges(0, 50, 20000, 0);
    List<LogEntryProto> entries = prepareLogEntries(ranges, null);

    try (SegmentedRaftLog raftLog = newSegmentedRaftLog()) {
      raftLog.open(RaftLog.INVALID_LOG_INDEX, null);
      // append entries to the raftlog
      List<List<CompletableFuture<Long>>> futures = new ArrayList<>();
      long start = System.nanoTime();
      for (int i = 0; i < entries.size(); i += 5) {
        // call append API
        List<LogEntryProto> entries1 = Arrays.asList(
            entries.get(i), entries.get(i + 1), entries.get(i + 2), entries.get(i + 3), entries.get(i + 4));
        futures.add(raftLog.append(ReferenceCountedObject.wrap(entries1)));
      }
      for (List<CompletableFuture<Long>> futureList: futures) {
        futureList.forEach(CompletableFuture::join);
      }
      System.out.println(entries.size() + " appendEntry finished in " + (System.nanoTime() - start) +
          " ns with asyncFlush " + useAsyncFlush);
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testAsyncFlushPerf2(Boolean useAsyncFlush, Boolean smSyncFlush) throws Exception {
    RaftServerConfigKeys.Log.setAsyncFlushEnabled(properties, useAsyncFlush);
    RaftServerConfigKeys.Log.StateMachineData.setSync(properties, smSyncFlush);
    List<SegmentRange> ranges = prepareRanges(0, 50, 20000, 0);
    List<LogEntryProto> entries = prepareLogEntries(ranges, null);

    try (SegmentedRaftLog raftLog = newSegmentedRaftLog()) {
      raftLog.open(RaftLog.INVALID_LOG_INDEX, null);
      // append entries to the raftlog
      List<CompletableFuture<Long>> futures = new ArrayList<>();
      long start = System.nanoTime();
      for (int i = 0; i < entries.size(); i++) {
        // call appendEntry API
        futures.add(raftLog.appendEntry(entries.get(i)));
      }
      for (CompletableFuture<Long> futureList: futures) {
        futureList.join();
      }
      System.out.println(entries.size() + " appendEntry finished in " + (System.nanoTime() - start) +
          " ns with asyncFlush " + useAsyncFlush);
    }
  }
}
