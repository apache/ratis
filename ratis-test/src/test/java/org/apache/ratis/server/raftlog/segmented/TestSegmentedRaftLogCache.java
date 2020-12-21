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

import java.io.IOException;
import java.util.Iterator;
import java.util.stream.IntStream;

import org.apache.ratis.RaftTestUtil.SimpleOperation;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.metrics.RatisMetricRegistry;
import org.apache.ratis.server.impl.RaftServerTestUtil;
import org.apache.ratis.server.metrics.SegmentedRaftLogMetrics;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.LogEntryHeader;
import org.apache.ratis.server.raftlog.LogProtoUtils;
import org.apache.ratis.server.raftlog.segmented.SegmentedRaftLogCache.TruncationSegments;
import org.apache.ratis.server.raftlog.segmented.LogSegment.LogRecord;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestSegmentedRaftLogCache {
  private static final RaftProperties prop = new RaftProperties();

  private SegmentedRaftLogCache cache;
  private SegmentedRaftLogMetrics raftLogMetrics;
  private RatisMetricRegistry ratisMetricRegistry;

  @Before
  public void setup() {
    raftLogMetrics = new SegmentedRaftLogMetrics(RaftServerTestUtil.TEST_MEMBER_ID);
    ratisMetricRegistry = raftLogMetrics.getRegistry();
    cache = new SegmentedRaftLogCache(null, null, prop, raftLogMetrics);
  }

  @After
  public void clear() {
    raftLogMetrics.unregister();
  }

  private LogSegment prepareLogSegment(long start, long end, boolean isOpen) {
    LogSegment s = LogSegment.newOpenSegment(null, start, null);
    for (long i = start; i <= end; i++) {
      SimpleOperation m = new SimpleOperation("m" + i);
      LogEntryProto entry = LogProtoUtils.toLogEntryProto(m.getLogEntryContent(), 0, i);
      s.appendToOpenSegment(entry, LogSegment.Op.WRITE_CACHE_WITHOUT_STATE_MACHINE_CACHE);
    }
    if (!isOpen) {
      s.close();
    }
    return s;
  }

  private void checkCache(long start, long end, int segmentSize) {
    Assert.assertEquals(start, cache.getStartIndex());
    Assert.assertEquals(end, cache.getEndIndex());

    for (long index = start; index <= end; index++) {
      final LogSegment segment = cache.getSegment(index);
      final LogRecord record = segment.getLogRecord(index);
      final LogEntryProto entry = segment.getEntryFromCache(record.getTermIndex());
      Assert.assertEquals(index, entry.getIndex());
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
    Assert.assertEquals(realEnd - offset, entries.length);
    for (long i = offset; i < realEnd; i++) {
      Assert.assertEquals(i, entries[(int) (i - offset)].getIndex());
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
      Assert.fail("should fail since there is gap between two segments");
    } catch (IllegalStateException ignored) {
    }

    LogSegment s2 = prepareLogSegment(101, 200, true);
    cache.addSegment(s2);
    checkCache(1, 200, 100);

    try {
      LogSegment s = prepareLogSegment(201, 202, true);
      cache.addSegment(s);
      Assert.fail("should fail since there is still an open segment in cache");
    } catch (IllegalStateException ignored) {
    }

    cache.rollOpenSegment(false);
    checkCache(1, 200, 100);

    try {
      LogSegment s = prepareLogSegment(202, 203, true);
      cache.addSegment(s);
      Assert.fail("should fail since there is gap between two segments");
    } catch (IllegalStateException ignored) {
    }

    LogSegment s3 = prepareLogSegment(201, 300, true);
    cache.addSegment(s3);
    Assert.assertNotNull(cache.getOpenSegment());
    checkCache(1, 300, 100);

    cache.rollOpenSegment(true);
    Assert.assertNotNull(cache.getOpenSegment());
    checkCache(1, 300, 100);
  }

  @Test
  public void testAppendEntry() throws Exception {
    LogSegment closedSegment = prepareLogSegment(0, 99, false);
    cache.addSegment(closedSegment);

    final SimpleOperation m = new SimpleOperation("m");
    try {
      LogEntryProto entry = LogProtoUtils.toLogEntryProto(m.getLogEntryContent(), 0, 0);
      cache.appendEntry(entry, LogSegment.Op.WRITE_CACHE_WITHOUT_STATE_MACHINE_CACHE);
      Assert.fail("the open segment is null");
    } catch (IllegalStateException ignored) {
    }

    LogSegment openSegment = prepareLogSegment(100, 100, true);
    cache.addSegment(openSegment);
    for (long index = 101; index < 200; index++) {
      LogEntryProto entry = LogProtoUtils.toLogEntryProto(m.getLogEntryContent(), 0, index);
      cache.appendEntry(entry, LogSegment.Op.WRITE_CACHE_WITHOUT_STATE_MACHINE_CACHE);
    }

    Assert.assertNotNull(cache.getOpenSegment());
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
    Assert.assertEquals(599, end);
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
        Assert.assertEquals(1, ts.getToDelete().length);
        numOfSegments = currentNum;
      } else {
        Assert.assertEquals(0, ts.getToDelete().length);
      }
    }

    // 230 entries remaining. truncate at the segment boundary
    TruncationSegments ts = cache.truncate(200);
    checkCache(0, 199, 100);
    Assert.assertEquals(1, ts.getToDelete().length);
    Assert.assertEquals(200, ts.getToDelete()[0].getStartIndex());
    Assert.assertEquals(229, ts.getToDelete()[0].getEndIndex());
    Assert.assertEquals(0, ts.getToDelete()[0].getTargetLength());
    Assert.assertFalse(ts.getToDelete()[0].isOpen());
    Assert.assertNull(ts.getToTruncate());

    // add another open segment and truncate it as a whole
    LogSegment newOpen = prepareLogSegment(200, 249, true);
    cache.addSegment(newOpen);
    ts = cache.truncate(200);
    checkCache(0, 199, 100);
    Assert.assertEquals(1, ts.getToDelete().length);
    Assert.assertEquals(200, ts.getToDelete()[0].getStartIndex());
    Assert.assertEquals(249, ts.getToDelete()[0].getEndIndex());
    Assert.assertEquals(0, ts.getToDelete()[0].getTargetLength());
    Assert.assertTrue(ts.getToDelete()[0].isOpen());
    Assert.assertNull(ts.getToTruncate());

    // add another open segment and truncate part of it
    newOpen = prepareLogSegment(200, 249, true);
    cache.addSegment(newOpen);
    ts = cache.truncate(220);
    checkCache(0, 219, 100);
    Assert.assertNull(cache.getOpenSegment());
    Assert.assertEquals(0, ts.getToDelete().length);
    Assert.assertTrue(ts.getToTruncate().isOpen());
    Assert.assertEquals(219, ts.getToTruncate().getNewEndIndex());
    Assert.assertEquals(200, ts.getToTruncate().getStartIndex());
    Assert.assertEquals(249, ts.getToTruncate().getEndIndex());
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
    Assert.assertNull(ts.getToTruncate());
    Assert.assertEquals(end - start, ts.getToDelete().length);
    Assert.assertEquals(sIndex, cache.getStartIndex());
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
    Assert.assertNull(ts.getToTruncate());
    Assert.assertEquals(end - start - 1, ts.getToDelete().length);
    Assert.assertEquals(1, cache.getNumOfSegments());
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
      Assert.assertEquals(cache.getLogRecord(termIndex.getIndex()).getTermIndex(), termIndex);
      if (prev != null) {
        Assert.assertEquals(prev.getIndex() + 1, termIndex.getIndex());
      }
      prev = termIndex;
    }
    if (startIndex <= cache.getEndIndex()) {
      Assert.assertNotNull(prev);
      Assert.assertEquals(cache.getEndIndex(), prev.getIndex());
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
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testCacheMetric() {
    cache.addSegment(prepareLogSegment(0, 99, false));
    cache.addSegment(prepareLogSegment(100, 200, false));
    cache.addSegment(prepareLogSegment(201, 300, true));

    Long closedSegmentsNum = (Long) ratisMetricRegistry.getGauges((s, metric) ->
        s.contains(RAFT_LOG_CACHE_CLOSED_SEGMENTS_NUM)).values().iterator().next().getValue();
    Assert.assertEquals(2L, closedSegmentsNum.longValue());

    Long closedSegmentsSizeInBytes = (Long) ratisMetricRegistry.getGauges((s, metric) ->
        s.contains(RAFT_LOG_CACHE_CLOSED_SEGMENTS_SIZE_IN_BYTES)).values().iterator().next().getValue();
    Assert.assertEquals(closedSegmentsSizeInBytes.longValue(), cache.getClosedSegmentsSizeInBytes());

    Long openSegmentSizeInBytes = (Long) ratisMetricRegistry.getGauges((s, metric) ->
        s.contains(RAFT_LOG_CACHE_OPEN_SEGMENT_SIZE_IN_BYTES)).values().iterator().next().getValue();
    Assert.assertEquals(openSegmentSizeInBytes.longValue(), cache.getOpenSegmentSizeInBytes());
  }
}
