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

import static org.apache.ratis.server.metrics.SegmentedRaftLogMetrics.*;
import static org.apache.ratis.server.raftlog.segmented.SegmentedRaftLogTestUtils.MAX_OP_SIZE;

import java.io.IOException;
import java.util.Iterator;
import java.util.stream.IntStream;

import org.apache.ratis.RaftTestUtil.SimpleOperation;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.metrics.impl.RatisMetricRegistryImpl;
import org.apache.ratis.server.impl.RaftServerTestUtil;
import org.apache.ratis.server.metrics.SegmentedRaftLogMetrics;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.LogEntryHeader;
import org.apache.ratis.server.raftlog.LogProtoUtils;
import org.apache.ratis.server.raftlog.segmented.SegmentedRaftLogCache.TruncationSegments;
import org.apache.ratis.server.raftlog.segmented.LogSegment.LogRecord;
import org.apache.ratis.server.raftlog.segmented.LogSegment.Op;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.util.ReferenceCountedObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestSegmentedRaftLogCache {
  private static final RaftProperties prop = new RaftProperties();

  private SegmentedRaftLogCache cache;
  private SegmentedRaftLogMetrics raftLogMetrics;
  private RatisMetricRegistryImpl ratisMetricRegistry;

  @BeforeEach
  public void setup() {
    raftLogMetrics = new SegmentedRaftLogMetrics(RaftServerTestUtil.TEST_MEMBER_ID);
    ratisMetricRegistry = (RatisMetricRegistryImpl) raftLogMetrics.getRegistry();
    cache = new SegmentedRaftLogCache(null, null, prop, raftLogMetrics);
  }

  @AfterEach
  public void clear() {
    raftLogMetrics.unregister();
  }

  private LogSegment prepareLogSegment(long start, long end, boolean isOpen) {
    LogSegment s = LogSegment.newOpenSegment(null, start, MAX_OP_SIZE, null);
    for (long i = start; i <= end; i++) {
      SimpleOperation m = new SimpleOperation("m" + i);
      LogEntryProto entry = LogProtoUtils.toLogEntryProto(m.getLogEntryContent(), 0, i);
      s.appendToOpenSegment(Op.WRITE_CACHE_WITHOUT_STATE_MACHINE_CACHE, ReferenceCountedObject.wrap(entry));
    }
    if (!isOpen) {
      s.close();
    }
    return s;
  }

  private void checkCache(long start, long end, int segmentSize) {
    Assertions.assertEquals(start, cache.getStartIndex());
    Assertions.assertEquals(end, cache.getEndIndex());

    for (long index = start; index <= end; index++) {
      final LogSegment segment = cache.getSegment(index);
      final LogRecord record = segment.getLogRecord(index);
      final ReferenceCountedObject<LogEntryProto> entry = segment.getEntryFromCache(record.getTermIndex());
      Assertions.assertEquals(index, entry.get().getIndex());
    }

    long[] offsets = new long[]{start, start + 1, start + (end - start) / 2,
        end - 1, end};
    for (long offset : offsets) {
      checkCacheEntries(offset, (int) (end - offset + 1), end);
      checkCacheEntries(offset, 1, end);
      checkCacheEntries(offset, 20, end);
      checkCacheEntries(offset, segmentSize, end);
      checkCacheEntries(offset, segmentSize - 1, end);
    }
  }

  private void checkCacheEntries(long offset, int size, long end) {
    final LogEntryHeader[] entries = cache.getTermIndices(offset, offset + size);
    long realEnd = offset + size > end + 1 ? end + 1 : offset + size;
    Assertions.assertEquals(realEnd - offset, entries.length);
    for (long i = offset; i < realEnd; i++) {
      Assertions.assertEquals(i, entries[(int) (i - offset)].getIndex());
    }
  }

  @Test
  public void testAddSegments() throws Exception {
    LogSegment s1 = prepareLogSegment(1, 100, false);
    cache.addSegment(s1);
    checkCache(1, 100, 100);

    try {
      LogSegment s = prepareLogSegment(102, 103, true);
      cache.addSegment(s);
      Assertions.fail("should fail since there is gap between two segments");
    } catch (IllegalStateException ignored) {
    }

    LogSegment s2 = prepareLogSegment(101, 200, true);
    cache.addSegment(s2);
    checkCache(1, 200, 100);

    try {
      LogSegment s = prepareLogSegment(201, 202, true);
      cache.addSegment(s);
      Assertions.fail("should fail since there is still an open segment in cache");
    } catch (IllegalStateException ignored) {
    }

    cache.rollOpenSegment(false);
    checkCache(1, 200, 100);

    try {
      LogSegment s = prepareLogSegment(202, 203, true);
      cache.addSegment(s);
      Assertions.fail("should fail since there is gap between two segments");
    } catch (IllegalStateException ignored) {
    }

    LogSegment s3 = prepareLogSegment(201, 300, true);
    cache.addSegment(s3);
    Assertions.assertNotNull(cache.getOpenSegment());
    checkCache(1, 300, 100);

    cache.rollOpenSegment(true);
    Assertions.assertNotNull(cache.getOpenSegment());
    checkCache(1, 300, 100);
  }

  @Test
  public void testAppendEntry() {
    LogSegment closedSegment = prepareLogSegment(0, 99, false);
    cache.addSegment(closedSegment);

    final SimpleOperation m = new SimpleOperation("m");
    try {
      LogEntryProto entry = LogProtoUtils.toLogEntryProto(m.getLogEntryContent(), 0, 0);
      cache.appendEntry(Op.WRITE_CACHE_WITHOUT_STATE_MACHINE_CACHE, ReferenceCountedObject.wrap(entry)
      );
      Assertions.fail("the open segment is null");
    } catch (IllegalStateException ignored) {
    }

    LogSegment openSegment = prepareLogSegment(100, 100, true);
    cache.addSegment(openSegment);
    for (long index = 101; index < 200; index++) {
      LogEntryProto entry = LogProtoUtils.toLogEntryProto(m.getLogEntryContent(), 0, index);
      cache.appendEntry(Op.WRITE_CACHE_WITHOUT_STATE_MACHINE_CACHE, ReferenceCountedObject.wrap(entry)
      );
    }

    Assertions.assertNotNull(cache.getOpenSegment());
    checkCache(0, 199, 100);
  }

  @Test
  public void testTruncate() throws Exception {
    long start = 0;
    for (int i = 0; i < 5; i++) { // 5 closed segments
      LogSegment s = prepareLogSegment(start, start + 99, false);
      cache.addSegment(s);
      start += 100;
    }
    // add another open segment
    LogSegment s = prepareLogSegment(start, start + 99, true);
    cache.addSegment(s);

    long end = cache.getEndIndex();
    Assertions.assertEquals(599, end);
    int numOfSegments = 6;
    // start truncation
    for (int i = 0; i < 10; i++) { // truncate 10 times
      // each time truncate 37 entries
      end -= 37;
      TruncationSegments ts = cache.truncate(end + 1);
      checkCache(0, end, 100);

      // check TruncationSegments
      int currentNum= (int) (end / 100 + 1);
      if (currentNum < numOfSegments) {
        Assertions.assertEquals(1, ts.getToDelete().length);
        numOfSegments = currentNum;
      } else {
        Assertions.assertEquals(0, ts.getToDelete().length);
      }
    }

    // 230 entries remaining. truncate at the segment boundary
    TruncationSegments ts = cache.truncate(200);
    checkCache(0, 199, 100);
    Assertions.assertEquals(1, ts.getToDelete().length);
    Assertions.assertEquals(200, ts.getToDelete()[0].getStartIndex());
    Assertions.assertEquals(229, ts.getToDelete()[0].getEndIndex());
    Assertions.assertEquals(0, ts.getToDelete()[0].getTargetLength());
    Assertions.assertFalse(ts.getToDelete()[0].isOpen());
    Assertions.assertNull(ts.getToTruncate());

    // add another open segment and truncate it as a whole
    LogSegment newOpen = prepareLogSegment(200, 249, true);
    cache.addSegment(newOpen);
    ts = cache.truncate(200);
    checkCache(0, 199, 100);
    Assertions.assertEquals(1, ts.getToDelete().length);
    Assertions.assertEquals(200, ts.getToDelete()[0].getStartIndex());
    Assertions.assertEquals(249, ts.getToDelete()[0].getEndIndex());
    Assertions.assertEquals(0, ts.getToDelete()[0].getTargetLength());
    Assertions.assertTrue(ts.getToDelete()[0].isOpen());
    Assertions.assertNull(ts.getToTruncate());

    // add another open segment and truncate part of it
    newOpen = prepareLogSegment(200, 249, true);
    cache.addSegment(newOpen);
    ts = cache.truncate(220);
    checkCache(0, 219, 100);
    Assertions.assertNull(cache.getOpenSegment());
    Assertions.assertEquals(0, ts.getToDelete().length);
    Assertions.assertTrue(ts.getToTruncate().isOpen());
    Assertions.assertEquals(219, ts.getToTruncate().getNewEndIndex());
    Assertions.assertEquals(200, ts.getToTruncate().getStartIndex());
    Assertions.assertEquals(249, ts.getToTruncate().getEndIndex());
  }

  @Test
  public void testOpenSegmentPurge() {
    int start = 0;
    int end = 5;
    int segmentSize = 100;
    populatedSegment(start, end, segmentSize, false);

    int sIndex = (end - start) * segmentSize;
    populatedSegment(end, end + 1, segmentSize, true);

    int purgeIndex = sIndex;
    // open segment should never be purged
    TruncationSegments ts = cache.purge(purgeIndex);
    Assertions.assertNull(ts.getToTruncate());
    Assertions.assertEquals(end - start, ts.getToDelete().length);
    Assertions.assertEquals(sIndex, cache.getStartIndex());
  }

  @Test
  public void testCloseSegmentPurge() {
    int start = 0;
    int end = 5;
    int segmentSize = 100;
    populatedSegment(start, end, segmentSize, false);

    int purgeIndex = (end - start) * segmentSize - 1;

    // overlapped close segment will not purged. Passing in index - 1 since
    // we purge a closed segment when end index == passed in purge index.
    TruncationSegments ts = cache.purge(purgeIndex - 1);
    Assertions.assertNull(ts.getToTruncate());
    Assertions.assertEquals(end - start - 1, ts.getToDelete().length);
    Assertions.assertEquals(1, cache.getNumOfSegments());
  }

  private void populatedSegment(int start, int end, int segmentSize, boolean isOpen) {
    IntStream.range(start, end).forEach(x -> {
      int startIndex = x * segmentSize;
      int endIndex = startIndex + segmentSize - 1;
      LogSegment s = prepareLogSegment(startIndex, endIndex, isOpen);
      cache.addSegment(s);
    });
  }

  private void testIterator(long startIndex) throws IOException {
    Iterator<TermIndex> iterator = cache.iterator(startIndex);
    TermIndex prev = null;
    while (iterator.hasNext()) {
      TermIndex termIndex = iterator.next();
      Assertions.assertEquals(cache.getLogRecord(termIndex.getIndex()).getTermIndex(), termIndex);
      if (prev != null) {
        Assertions.assertEquals(prev.getIndex() + 1, termIndex.getIndex());
      }
      prev = termIndex;
    }
    if (startIndex <= cache.getEndIndex()) {
      Assertions.assertNotNull(prev);
      Assertions.assertEquals(cache.getEndIndex(), prev.getIndex());
    }
  }

  @Test
  public void testIterator() throws Exception {
    long start = 0;
    for (int i = 0; i < 2; i++) { // 2 closed segments
      LogSegment s = prepareLogSegment(start, start + 99, false);
      cache.addSegment(s);
      start += 100;
    }
    // add another open segment
    LogSegment s = prepareLogSegment(start, start + 99, true);
    cache.addSegment(s);

    for (long startIndex = 0; startIndex < 300; startIndex += 50) {
      testIterator(startIndex);
    }
    testIterator(299);

    Iterator<TermIndex> iterator = cache.iterator(300);
    Assertions.assertFalse(iterator.hasNext());
  }

  @Test
  public void testCacheMetric() {
    cache.addSegment(prepareLogSegment(0, 99, false));
    cache.addSegment(prepareLogSegment(100, 200, false));
    cache.addSegment(prepareLogSegment(201, 300, true));

    Long closedSegmentsNum = (Long) ratisMetricRegistry.getGauges((s, metric) ->
        s.contains(RAFT_LOG_CACHE_CLOSED_SEGMENTS_NUM)).values().iterator().next().getValue();
    Assertions.assertEquals(2L, closedSegmentsNum.longValue());

    Long closedSegmentsSizeInBytes = (Long) ratisMetricRegistry.getGauges((s, metric) ->
        s.contains(RAFT_LOG_CACHE_CLOSED_SEGMENTS_SIZE_IN_BYTES)).values().iterator().next().getValue();
    Assertions.assertEquals(closedSegmentsSizeInBytes.longValue(), cache.getClosedSegmentsSizeInBytes());

    Long openSegmentSizeInBytes = (Long) ratisMetricRegistry.getGauges((s, metric) ->
        s.contains(RAFT_LOG_CACHE_OPEN_SEGMENT_SIZE_IN_BYTES)).values().iterator().next().getValue();
    Assertions.assertEquals(openSegmentSizeInBytes.longValue(), cache.getOpenSegmentSizeInBytes());
  }
}
