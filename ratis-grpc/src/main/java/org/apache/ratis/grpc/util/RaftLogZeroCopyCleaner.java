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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * When using {@link ZeroCopyMessageMarshaller} in services that receive Raft logs, e.g. GrpcClientProtocolService
 * or GrpcServerProtocolService, the original message buffers can't be released immediately after onNext() because
 * the protobuf log messages is kept in RaftLogCache waiting to be applied to state machine or replicated
 * to followers.
 *
 * <p>
 * This component manages the buffers to close, sorts them by the associating log index and release them safely by
 * observing cache evict event.
 *
 * @see {@link org.apache.ratis.grpc.server.GrpcClientProtocolService}
 * @see {@link org.apache.ratis.grpc.server.GrpcServerProtocolService}
 */
public abstract class RaftLogZeroCopyCleaner {
  public static final Logger LOG = LoggerFactory.getLogger(RaftLogZeroCopyCleaner.class);

  public abstract void watch(RaftClientReply clientReply, Closeable handle);
  public abstract void release(RaftGroupId groupId, long startIndex, long endIndex);
  public abstract void releaseAll(RaftGroupId groupId);

  public static RaftLogZeroCopyCleaner create(boolean zeroCopyEnabled) {
    return zeroCopyEnabled ? new RaftLogZeroCopyCleanerImpl() : new NoopRaftLogZeroCopyCleaner();
  }

  public static void close(Closeable closeable) {
    GrpcUtil.closeQuietly(closeable);
  }

  private static class NoopRaftLogZeroCopyCleaner extends RaftLogZeroCopyCleaner {
    @Override
    public void release(RaftGroupId groupId, long startIndex, long endIndex) {
    }

    @Override
    public void releaseAll(RaftGroupId groupId) {
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
    public void release(RaftGroupId groupId, long startIndex, long endIndex) {
      if (startIndex > endIndex) {
        return ;
      }

      runInGroupSequentially(groupId, group -> {
        NavigableMap<Long, ReferenceCountedClosable> range = group.subMap(startIndex, true, endIndex, true);
        List<Long> removedIndices = new LinkedList<>();
        for (Map.Entry<Long, ReferenceCountedClosable> entry : range.entrySet()) {
          Long idx = entry.getKey();
          ReferenceCountedClosable closable = entry.getValue();
          release(closable);
          removedIndices.add(idx);
        }
        removedIndices.forEach(group::remove);
        LOG.debug("{} - Released {}->{}, {} entries hit", groupId, startIndex, endIndex, removedIndices.size());
      });
    }

    @Override
    public void releaseAll(RaftGroupId groupId) {
      NavigableMap<Long, ReferenceCountedClosable> removed = groups.remove(groupId);
      if (removed != null) {
        removed.values().forEach(x -> release(x));
        LOG.debug("{} - Released all, {} entries hit", groupId, removed.size());
      }
    }

    @Override
    public void watch(RaftClientReply clientReply, Closeable handle) {
      if (handle == null) {
        return;
      }
      ReferenceCountedClosable ref = new ReferenceCountedClosable(1, handle);
      runInGroupSequentially(clientReply.getRaftGroupId(), group -> {
        LOG.debug("{} - Watch {}, size {} bytes", clientReply.getRaftGroupId(), clientReply.getLogIndex(), ref.size);
        ReferenceCountedClosable overwritten = group.put(clientReply.getLogIndex(), ref);
        release(overwritten);
      });
    }

    private void runInGroupSequentially(RaftGroupId groupId,
        Consumer<NavigableMap<Long, ReferenceCountedClosable>> block) {
      NavigableMap<Long, ReferenceCountedClosable> group = groups.computeIfAbsent(groupId, (k) -> new TreeMap<>());
      synchronized (group) {
        block.accept(group);
      }
    }

    private static void release(ReferenceCountedClosable closable) {
      if (closable == null) {
        return;
      }
      if (closable.refCount.decrementAndGet() <= 0) {
        close(closable.handle);
      }
    }

  }

  private static final class ReferenceCountedClosable {
    private final AtomicInteger refCount;
    private final Closeable handle;
    private final int size;

    private ReferenceCountedClosable(int refCount, Closeable handle) {
      this.refCount = new AtomicInteger(refCount);
      this.handle = handle;
      this.size = estimateSize(handle);
    }

    private static int estimateSize(Closeable handle) {
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
