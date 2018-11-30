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
package org.apache.ratis.server.storage;

import org.apache.ratis.BaseTest;
import org.apache.ratis.MiniRaftCluster;
import org.apache.ratis.RaftTestUtil.SimpleOperation;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.RaftServerConstants;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.impl.ServerProtoUtils;
import org.apache.ratis.server.impl.ServerState;
import org.apache.ratis.server.storage.CacheInvalidationPolicy.CacheInvalidationPolicyDefault;
import org.apache.ratis.server.storage.RaftLogCache.LogSegmentList;
import org.apache.ratis.server.storage.TestSegmentedRaftLog.SegmentRange;
import org.apache.ratis.statemachine.SimpleStateMachine4Testing;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.SizeInBytes;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class TestCacheEviction extends BaseTest {
  private static final CacheInvalidationPolicy policy = new CacheInvalidationPolicyDefault();

  static LogSegmentList prepareSegments(int numSegments, boolean[] cached, long start, long size) {
    Assert.assertEquals(numSegments, cached.length);
    final LogSegmentList segments = new LogSegmentList(TestCacheEviction.class.getSimpleName());
    for (int i = 0; i < numSegments; i++) {
      LogSegment s = LogSegment.newCloseSegment(null, start, start + size - 1);
      if (cached[i]) {
        s = Mockito.spy(s);
        Mockito.when(s.hasCache()).thenReturn(true);
      }
      segments.add(s);
      start += size;
    }
    return segments;
  }

  @Test
  public void testBasicEviction() throws Exception {
    final int maxCached = 5;
    final LogSegmentList segments = prepareSegments(5,
        new boolean[]{true, true, true, true, true}, 0, 10);

    // case 1, make sure we do not evict cache for segments behind local flushed index
    List<LogSegment> evicted = policy.evict(null, 5, 15, segments, maxCached);
    Assert.assertEquals(0, evicted.size());

    // case 2, suppose the local flushed index is in the 3rd segment, then we
    // can evict the first two segment
    evicted = policy.evict(null, 25, 30, segments, maxCached);
    Assert.assertEquals(2, evicted.size());
    Assert.assertSame(evicted.get(0), segments.get(0));
    Assert.assertSame(evicted.get(1), segments.get(1));

    // case 3, similar with case 2, but the local applied index is less than
    // the local flushed index.
    evicted = policy.evict(null, 25, 15, segments, maxCached);
    Assert.assertEquals(1, evicted.size());
    Assert.assertSame(evicted.get(0), segments.get(0));

    // case 4, the local applied index is very small, then evict cache behind it
    // first and let the state machine load the segments later
    evicted = policy.evict(null, 35, 5, segments, maxCached);
    Assert.assertEquals(1, evicted.size());
    Assert.assertSame(evicted.get(0), segments.get(2));

    Mockito.when(segments.get(2).hasCache()).thenReturn(false);
    evicted = policy.evict(null, 35, 5, segments, maxCached);
    Assert.assertEquals(1, evicted.size());
    Assert.assertSame(evicted.get(0), segments.get(1));

    Mockito.when(segments.get(1).hasCache()).thenReturn(false);
    evicted = policy.evict(null, 35, 5, segments, maxCached);
    Assert.assertEquals(0, evicted.size());
  }

  @Test
  public void testEvictionWithFollowerIndices() throws Exception {
    final int maxCached = 6;
    final LogSegmentList segments = prepareSegments(6,
        new boolean[]{true, true, true, true, true, true}, 0, 10);

    // case 1, no matter where the followers are, we do not evict segments behind local
    // flushed index
    List<LogSegment> evicted = policy.evict(new long[]{20, 40, 40}, 5, 15, segments,
        maxCached);
    Assert.assertEquals(0, evicted.size());

    // case 2, the follower indices are behind the local flushed index
    evicted = policy.evict(new long[]{30, 40, 45}, 25, 30, segments, maxCached);
    Assert.assertEquals(2, evicted.size());
    Assert.assertSame(evicted.get(0), segments.get(0));
    Assert.assertSame(evicted.get(1), segments.get(1));

    // case 3, similar with case 3 in basic eviction test
    evicted = policy.evict(new long[]{30, 40, 45}, 25, 15, segments, maxCached);
    Assert.assertEquals(1, evicted.size());
    Assert.assertSame(evicted.get(0), segments.get(0));

    // case 4, the followers are slower than local flush
    evicted = policy.evict(new long[]{15, 45, 45}, 55, 50, segments, maxCached);
    Assert.assertEquals(1, evicted.size());
    Assert.assertSame(evicted.get(0), segments.get(0));

    Mockito.when(segments.get(0).hasCache()).thenReturn(false);
    evicted = policy.evict(new long[]{15, 45, 45}, 55, 50, segments, maxCached);
    Assert.assertEquals(1, evicted.size());
    Assert.assertSame(evicted.get(0), segments.get(2));

    Mockito.when(segments.get(2).hasCache()).thenReturn(false);
    evicted = policy.evict(new long[]{15, 45, 45}, 55, 50, segments, maxCached);
    Assert.assertEquals(1, evicted.size());
    Assert.assertSame(evicted.get(0), segments.get(3));

    Mockito.when(segments.get(3).hasCache()).thenReturn(false);
    evicted = policy.evict(new long[]{15, 45, 45}, 55, 50, segments, maxCached);
    Assert.assertEquals(0, evicted.size());
  }

  @Test
  public void testEvictionInSegmentedLog() throws Exception {
    final RaftProperties prop = new RaftProperties();
    prop.setClass(MiniRaftCluster.STATEMACHINE_CLASS_KEY,
        SimpleStateMachine4Testing.class, StateMachine.class);
    RaftServerConfigKeys.Log.setSegmentSizeMax(prop, SizeInBytes.valueOf("8KB"));
    RaftServerConfigKeys.Log.setPreallocatedSize(prop, SizeInBytes.valueOf("8KB"));
    final RaftPeerId peerId = RaftPeerId.valueOf("s0");
    final int maxCachedNum = RaftServerConfigKeys.Log.maxCachedSegmentNum(prop);

    File storageDir = getTestDir();
    RaftServerConfigKeys.setStorageDirs(prop,  Collections.singletonList(storageDir));
    RaftStorage storage = new RaftStorage(storageDir, RaftServerConstants.StartupOption.REGULAR);

    RaftServerImpl server = Mockito.mock(RaftServerImpl.class);
    ServerState state = Mockito.mock(ServerState.class);
    Mockito.when(server.getState()).thenReturn(state);
    Mockito.when(server.getFollowerNextIndices()).thenReturn(new long[]{});
    Mockito.when(state.getLastAppliedIndex()).thenReturn(0L);

    SegmentedRaftLog raftLog = new SegmentedRaftLog(peerId, server, storage, -1, prop);
    raftLog.open(RaftServerConstants.INVALID_LOG_INDEX, null);
    List<SegmentRange> slist = TestSegmentedRaftLog.prepareRanges(0, maxCachedNum, 7, 0);
    LogEntryProto[] entries = generateEntries(slist);
    raftLog.append(entries).forEach(CompletableFuture::join);

    // check the current cached segment number: the last segment is still open
    Assert.assertEquals(maxCachedNum - 1,
        raftLog.getRaftLogCache().getCachedSegmentNum());

    Mockito.when(server.getFollowerNextIndices()).thenReturn(new long[]{21, 40, 40});
    Mockito.when(state.getLastAppliedIndex()).thenReturn(35L);
    slist = TestSegmentedRaftLog.prepareRanges(maxCachedNum, maxCachedNum + 2, 7, 7 * maxCachedNum);
    entries = generateEntries(slist);
    raftLog.append(entries).forEach(CompletableFuture::join);

    // check the cached segment number again. since the slowest follower is on
    // index 21, the eviction should happen and evict 3 segments
    Assert.assertEquals(maxCachedNum + 1 - 3,
        raftLog.getRaftLogCache().getCachedSegmentNum());
  }

  private LogEntryProto[] generateEntries(List<SegmentRange> slist) {
    List<LogEntryProto> eList = new ArrayList<>();
    for (SegmentRange range : slist) {
      for (long index = range.start; index <= range.end; index++) {
        SimpleOperation m = new SimpleOperation(new String(new byte[1024]));
        eList.add(ServerProtoUtils.toLogEntryProto(m.getLogEntryContent(), range.term, index));
      }
    }
    return eList.toArray(new LogEntryProto[eList.size()]);
  }
}
