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
package org.apache.ratis.grpc.util;

import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.thirdparty.io.grpc.KnownLength;
import org.apache.ratis.thirdparty.io.grpc.internal.GrpcUtil;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * When using {@link ZeroCopyMessageMarshaller} in services that receive Raft logs, e.g. GrpcClientProtocolService
 * or GrpcServerProtocolService, the original message buffers can't be released immediately after onNext() because
 * the protobuf log messages is kept in Ratis log cache (or StateMachine cache) waiting for being applied to state
 * machine or replicated to followers.
 *
 * <p>
 * This component manages the buffers to close, sorts them by the associating log index and release them safely by
 * observing events like index applied.
 *
 * @see {@link org.apache.ratis.grpc.server.GrpcClientProtocolService}
 * @see {@link org.apache.ratis.grpc.server.GrpcServerProtocolService}
 */
public abstract class RaftLogZeroCopyCleaner {
  public abstract void watch(RaftClientReply clientReply, Closeable handle);
  public abstract void onIndexChanged(RaftGroupId groupId, long appliedIndex, long[] followerIndices);
  public abstract void onServerClosed(RaftGroupId groupId);

  public static RaftLogZeroCopyCleaner create(boolean zeroCopyEnabled) {
    return zeroCopyEnabled ? new RaftLogZeroCopyCleanerImpl() : new NoopRaftLogZeroCopyCleaner();
  }

  public static void close(Closeable closeable) {
    GrpcUtil.closeQuietly(closeable);
  }

  private static class NoopRaftLogZeroCopyCleaner extends RaftLogZeroCopyCleaner {
    @Override
    public void onIndexChanged(RaftGroupId groupId, long appliedIndex, long[] followerIndices) {
    }

    @Override
    public void onServerClosed(RaftGroupId groupId) {
    }

    @Override
    public void watch(RaftClientReply clientReply, Closeable handle) {
    }
  }

  private static class RaftLogZeroCopyCleanerImpl extends RaftLogZeroCopyCleaner {
    private final Map<RaftGroupId, NavigableMap<Long, ReferenceCountedClosable>> groups = new ConcurrentHashMap<>();

    public RaftLogZeroCopyCleanerImpl() {
    }

    @Override
    public void onIndexChanged(RaftGroupId groupId, long appliedIndex, long[] followerNextIndices) {
      if (followerNextIndices != null) {
        long minFollowerNextIndex = Arrays.stream(followerNextIndices).min().orElse(Long.MAX_VALUE);
        long minIndex = Math.min(appliedIndex, minFollowerNextIndex - 1);
        release(groupId, 0, minIndex);
      } else {
        release(groupId, 0, appliedIndex);
      }
    }

    @Override
    public void onServerClosed(RaftGroupId groupId) {
      NavigableMap<Long, ReferenceCountedClosable> removed = groups.remove(groupId);
      removed.values().forEach(x -> release(x));
    }

    @Override
    public void watch(RaftClientReply clientReply, Closeable handle) {
      if (handle == null) {
        return;
      }
      ReferenceCountedClosable ref = new ReferenceCountedClosable(1, handle);
      NavigableMap<Long, ReferenceCountedClosable> group = group(clientReply.getRaftGroupId());
      synchronized (group) {
        ReferenceCountedClosable overwritten = group.put(clientReply.getLogIndex(), ref);
        release(overwritten);
      }
    }

    private NavigableMap<Long, ReferenceCountedClosable> group(RaftGroupId groupId) {
      return groups.computeIfAbsent(groupId, (k) -> new TreeMap<>());
    }

    private void release(RaftGroupId groupId, long startIndex, long endIndex) {
      if (startIndex > endIndex) {
        return ;
      }
      NavigableMap<Long, ReferenceCountedClosable> group = group(groupId);
      synchronized (group) {
        NavigableMap<Long, ReferenceCountedClosable> range = group.subMap(startIndex, true, endIndex, true);
        List<Long> removedIndices = new LinkedList<>();
        for (Map.Entry<Long, ReferenceCountedClosable> entry : range.entrySet()) {
          Long idx = entry.getKey();
          ReferenceCountedClosable closable = entry.getValue();
          release(closable);
          removedIndices.add(idx);
        }
        removedIndices.forEach(group::remove);
      }
    }

    private void release(ReferenceCountedClosable closable) {
      if (closable == null) {
        return;
      }
      if (closable.refCount.decrementAndGet() <= 0) {
        close(closable.handle);
      }
    }

  }

  private static class ReferenceCountedClosable {
    final AtomicInteger refCount;
    final Closeable handle;
    final int size;

    private ReferenceCountedClosable(int refCount, Closeable handle) {
      this.refCount = new AtomicInteger(refCount);
      this.handle = handle;
      this.size = estimateSize(handle);
    }

    private int estimateSize(Closeable handle) {
      if (handle instanceof KnownLength) {
        try {
          return ((KnownLength) handle).available();
        } catch (IOException e) {
          return 0;
        }
      }
      return 0;
    }
  }

}
