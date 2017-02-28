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
package org.apache.ratis.server.storage;

import java.util.Iterator;

import org.apache.ratis.RaftTestUtil.SimpleOperation;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.server.storage.RaftLogCache.TruncationSegments;
import org.apache.ratis.shaded.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.util.ProtoUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestRaftLogCache {
  private static final ClientId clientId = ClientId.createId();
  private static final long callId = 0;

  private RaftLogCache cache;

  @Before
  public void setup() {
    cache = new RaftLogCache();
  }

  private LogSegment prepareLogSegment(long start, long end, boolean isOpen) {
    LogSegment s = LogSegment.newOpenSegment(start);
    for (long i = start; i <= end; i++) {
      SimpleOperation m = new SimpleOperation("m" + i);
      LogEntryProto entry = ProtoUtils.toLogEntryProto(m.getLogEntryContent(),
          0, i, clientId, callId);
      s.appendToOpenSegment(entry);
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
      LogEntryProto entry = cache.getEntry(index);
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
    LogEntryProto[] entries = cache.getEntries(offset, offset + size);
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
      LogEntryProto entry = ProtoUtils.toLogEntryProto(m.getLogEntryContent(),
          0, 0, clientId, callId);
      cache.appendEntry(entry);
      Assert.fail("the open segment is null");
    } catch (IllegalStateException ignored) {
    }

    LogSegment openSegment = prepareLogSegment(100, 100, true);
    cache.addSegment(openSegment);
    for (long index = 101; index < 200; index++) {
      LogEntryProto entry = ProtoUtils.toLogEntryProto(m.getLogEntryContent(),
          0, index, clientId, callId);
      cache.appendEntry(entry);
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
        Assert.assertEquals(1, ts.toDelete.length);
        numOfSegments = currentNum;
      } else {
        Assert.assertEquals(0, ts.toDelete.length);
      }
    }

    // 230 entries remaining. truncate at the segment boundary
    TruncationSegments ts = cache.truncate(200);
    checkCache(0, 199, 100);
    Assert.assertEquals(1, ts.toDelete.length);
    Assert.assertEquals(200, ts.toDelete[0].startIndex);
    Assert.assertEquals(229, ts.toDelete[0].endIndex);
    Assert.assertEquals(0, ts.toDelete[0].targetLength);
    Assert.assertFalse(ts.toDelete[0].isOpen);
    Assert.assertNull(ts.toTruncate);

    // add another open segment and truncate it as a whole
    LogSegment newOpen = prepareLogSegment(200, 249, true);
    cache.addSegment(newOpen);
    ts = cache.truncate(200);
    checkCache(0, 199, 100);
    Assert.assertEquals(1, ts.toDelete.length);
    Assert.assertEquals(200, ts.toDelete[0].startIndex);
    Assert.assertEquals(249, ts.toDelete[0].endIndex);
    Assert.assertEquals(0, ts.toDelete[0].targetLength);
    Assert.assertTrue(ts.toDelete[0].isOpen);
    Assert.assertNull(ts.toTruncate);

    // add another open segment and truncate part of it
    newOpen = prepareLogSegment(200, 249, true);
    cache.addSegment(newOpen);
    ts = cache.truncate(220);
    checkCache(0, 219, 100);
    Assert.assertNull(cache.getOpenSegment());
    Assert.assertEquals(0, ts.toDelete.length);
    Assert.assertTrue(ts.toTruncate.isOpen);
    Assert.assertEquals(219, ts.toTruncate.newEndIndex);
    Assert.assertEquals(200, ts.toTruncate.startIndex);
    Assert.assertEquals(249, ts.toTruncate.endIndex);
  }

  private void testIterator(long startIndex) {
    Iterator<LogEntryProto> iterator = cache.iterator(startIndex);
    LogEntryProto prev = null;
    while (iterator.hasNext()) {
      LogEntryProto entry = iterator.next();
      Assert.assertEquals(cache.getEntry(entry.getIndex()), entry);
      if (prev != null) {
        Assert.assertEquals(prev.getIndex() + 1, entry.getIndex());
      }
      prev = entry;
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

    Iterator<LogEntryProto> iterator = cache.iterator(300);
    Assert.assertFalse(iterator.hasNext());
  }
}
