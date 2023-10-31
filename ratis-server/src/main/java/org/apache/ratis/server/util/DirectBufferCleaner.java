package org.apache.ratis.server.util;

import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.ratis.thirdparty.io.grpc.KnownLength;
import org.apache.ratis.thirdparty.io.grpc.internal.GrpcUtil;
import org.apache.ratis.util.ProtoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class DirectBufferCleaner {
  private static final Logger LOG = LoggerFactory.getLogger(
      DirectBufferCleaner.class);
  public static final DirectBufferCleaner INSTANCE = new DirectBufferCleaner();

  private Map<RaftGroupId, NavigableMap<Long, ReferenceCountedClosable>> groups = new ConcurrentHashMap<>();
  private final AtomicLong totalPendingInBytes = new AtomicLong();

  private ScheduledExecutorService executorService = Executors.newScheduledThreadPool(
      1, r -> {
        Thread t = new Thread(r);
        t.setName("DirectBufferCleanerWatcher");
        t.setDaemon(true);
        return t;
      });

  DirectBufferCleaner() {
    executorService.scheduleAtFixedRate(() -> {
      StringBuilder report = new StringBuilder();
      groups.forEach((gid, group) -> report.append(gid).append(" -> ").append(group.size()).append("\n"));
      LOG.info("Pending resources {}, total size {}mb, details: \n{}", pendingSize(), totalPendingInBytes.get() / 1024/ 1024, report);
    }, 5, 5, TimeUnit.SECONDS);
  }

  public int pendingSize() {
    return groups.values().stream().mapToInt(Map::size).sum();
  }

  @VisibleForTesting
  public Map<RaftGroupId, Set<Long>> pendingDetails() {
    return groups.entrySet().stream().filter(x -> x.getValue().size() > 0).collect(Collectors.toMap(Map.Entry::getKey, x -> x.getValue().keySet()));
  }

  @VisibleForTesting
  public void clear() {
    groups.clear();
  }

  private NavigableMap<Long, ReferenceCountedClosable> group(RaftGroupId groupId) {
    return groups.computeIfAbsent(groupId, (k) -> new TreeMap<>());
  }

  public boolean watch(RaftClientReply clientReply, Closeable handle) {
    if (handle == null) {
      return false;
    }
    ReferenceCountedClosable ref = new ReferenceCountedClosable(1, handle);
    NavigableMap<Long, ReferenceCountedClosable> group = group(clientReply.getRaftGroupId());
    totalPendingInBytes.addAndGet(ref.size);
    synchronized (group) {
      ReferenceCountedClosable overwritten = group.put(clientReply.getLogIndex(), ref);
      clean(overwritten);
    }
    return true;
  }

  public boolean watch(RaftProtos.AppendEntriesRequestProto request, InputStream handle) {
    if (handle == null) {
      return false;
    }
    RaftGroupId groupId = ProtoUtils.toRaftGroupId(request.getServerRequest().getRaftGroupId());
    NavigableMap<Long, ReferenceCountedClosable> group = group(groupId);
    ReferenceCountedClosable ref = new ReferenceCountedClosable(request.getEntriesCount(), handle);
    totalPendingInBytes.addAndGet(ref.size);
    synchronized (group) {
      request.getEntriesList().forEach(e -> {
        ReferenceCountedClosable overwritten = group.put(e.getIndex(), ref);
        clean(overwritten);
      });
    }
    return true;
  }

  public boolean clean(RaftGroupId groupId, LogEntryProto logProto) {
    LOG.info("{} - CLEAN: {}", groupId, logProto.getIndex());
    NavigableMap<Long, ReferenceCountedClosable> group = group(groupId);
    synchronized (group) {
      ReferenceCountedClosable closable = group.remove(logProto.getIndex());
      return clean(closable);
    }
  }

  private boolean clean(ReferenceCountedClosable closable) {
    if (closable == null) {
      return false;
    }
    if (closable.refCount.decrementAndGet() <= 0) {
      GrpcUtil.closeQuietly(closable.handle);
    }
    return true;
  }

  public boolean clean(RaftGroupId groupId, long startIdx, long endIdx) {
    NavigableMap<Long, ReferenceCountedClosable> group = group(groupId);
    synchronized (group) {
      NavigableMap<Long, ReferenceCountedClosable> range =
          group.subMap(startIdx, true, endIdx, true);
      List<Long> removedIndices = new LinkedList<>();

      long totalSize = 0;
      for (Map.Entry<Long, ReferenceCountedClosable> entry : range.entrySet()) {
        Long idx = entry.getKey();
        ReferenceCountedClosable closable = entry.getValue();
        if (closable.refCount.decrementAndGet() == 0) {
          GrpcUtil.closeQuietly(closable.handle);
          totalPendingInBytes.addAndGet(-closable.size);
        }
        removedIndices.add(idx);
        totalSize += entry.getValue().size;
      }
      removedIndices.forEach(group::remove);

      LOG.info("{} - CLEANED: {} -> {} : {} entries, total size {}mb", groupId, startIdx, endIdx, removedIndices.size(), totalSize / 1024 / 1024);

      int sizeBefore = group.subMap(0L, startIdx).size();
      if (sizeBefore > 0) {
        LOG.info("{} - Found {} uncleaned entries before the clean range {} -> {}", groupId, sizeBefore, startIdx, endIdx);
      }
      return !removedIndices.isEmpty();
    }
  }

  public void clean(RaftGroupId groupId, List<LogEntryProto> entries) {
    entries.forEach(entry -> this.clean(groupId, entry));
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
