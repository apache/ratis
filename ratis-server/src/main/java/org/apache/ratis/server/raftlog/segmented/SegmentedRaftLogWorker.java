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

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.metrics.RatisMetricsRegistry;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.TimeoutIOException;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.RaftServerConstants;
import org.apache.ratis.server.impl.ServerProtoUtils;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.server.raftlog.segmented.SegmentedRaftLogCache.SegmentFileInfo;
import org.apache.ratis.server.raftlog.segmented.SegmentedRaftLogCache.TruncationSegments;
import org.apache.ratis.server.raftlog.segmented.SegmentedRaftLog.Task;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * This class takes the responsibility of all the raft log related I/O ops for a
 * raft peer.
 */
class SegmentedRaftLogWorker implements Runnable {
  static final Logger LOG = LoggerFactory.getLogger(SegmentedRaftLogWorker.class);

  static final TimeDuration ONE_SECOND = TimeDuration.valueOf(1, TimeUnit.SECONDS);

  static class StateMachineDataPolicy {
    private final boolean sync;
    private final TimeDuration syncTimeout;
    private final int syncTimeoutRetry;

    StateMachineDataPolicy(RaftProperties properties) {
      this.sync = RaftServerConfigKeys.Log.StateMachineData.sync(properties);
      this.syncTimeout = RaftServerConfigKeys.Log.StateMachineData.syncTimeout(properties);
      this.syncTimeoutRetry = RaftServerConfigKeys.Log.StateMachineData.syncTimeoutRetry(properties);
      Preconditions.assertTrue(syncTimeoutRetry >= -1);
    }

    boolean isSync() {
      return sync;
    }

    void getFromFuture(CompletableFuture<?> future, Supplier<Object> getName) throws IOException {
      Preconditions.assertTrue(isSync());
      TimeoutIOException lastException = null;
      for(int retry = 0; syncTimeoutRetry == -1 || retry <= syncTimeoutRetry; retry++) {
        try {
          IOUtils.getFromFuture(future, getName, syncTimeout);
          return;
        } catch(TimeoutIOException e) {
          LOG.warn("Timeout " + retry + (syncTimeoutRetry == -1? "/~": "/" + syncTimeoutRetry), e);
          lastException = e;
        }
      }
      Objects.requireNonNull(lastException, "lastException == null");
      throw lastException;
    }
  }

  private final String name;
  /**
   * The task queue accessed by rpc handler threads and the io worker thread.
   */
  private final DataBlockingQueue<Task> queue;
  private volatile boolean running = true;
  private final Thread workerThread;

  private final RaftStorage storage;
  private volatile SegmentedRaftLogOutputStream out;
  private final Runnable submitUpdateCommitEvent;
  private final StateMachine stateMachine;
  private final Supplier<Timer> logFlushTimer;

  /**
   * The number of entries that have been written into the SegmentedRaftLogOutputStream but
   * has not been flushed.
   */
  private int pendingFlushNum = 0;
  /** the index of the last entry that has been written */
  private long lastWrittenIndex;
  /** the largest index of the entry that has been flushed */
  private volatile long flushedIndex;

  private final int forceSyncNum;

  private final long segmentMaxSize;
  private final long preallocatedSize;
  private final int bufferSize;

  private final StateMachineDataPolicy stateMachineDataPolicy;

  SegmentedRaftLogWorker(RaftPeerId selfId, StateMachine stateMachine, Runnable submitUpdateCommitEvent,
      RaftStorage storage, RaftProperties properties) {
    this.name = selfId + "-" + getClass().getSimpleName();
    LOG.info("new {} for {}", name, storage);

    this.submitUpdateCommitEvent = submitUpdateCommitEvent;
    this.stateMachine = stateMachine;

    this.storage = storage;

    final SizeInBytes queueByteLimit = RaftServerConfigKeys.Log.queueByteLimit(properties);
    final int queueElementLimit = RaftServerConfigKeys.Log.queueElementLimit(properties);
    this.queue = new DataBlockingQueue<>(name, queueByteLimit, queueElementLimit, Task::getSerializedSize);

    this.segmentMaxSize = RaftServerConfigKeys.Log.segmentSizeMax(properties).getSize();
    this.preallocatedSize = RaftServerConfigKeys.Log.preallocatedSize(properties).getSize();
    this.bufferSize = RaftServerConfigKeys.Log.writeBufferSize(properties).getSizeInt();
    this.forceSyncNum = RaftServerConfigKeys.Log.forceSyncNum(properties);

    this.stateMachineDataPolicy = new StateMachineDataPolicy(properties);

    this.workerThread = new Thread(this, name);

    // Server Id can be null in unit tests
    this.logFlushTimer = JavaUtils.memoize(() -> RatisMetricsRegistry.getRegistry()
        .timer(MetricRegistry.name(SegmentedRaftLogWorker.class, selfId.toString(), "flush-time")));
  }

  void start(long latestIndex, File openSegmentFile) throws IOException {
    LOG.trace("{} start(latestIndex={}, openSegmentFile={})", name, latestIndex, openSegmentFile);
    lastWrittenIndex = latestIndex;
    flushedIndex = latestIndex;
    if (openSegmentFile != null) {
      Preconditions.assertTrue(openSegmentFile.exists());
      out = new SegmentedRaftLogOutputStream(openSegmentFile, true, segmentMaxSize,
          preallocatedSize, bufferSize);
    }
    workerThread.start();
  }

  void close() {
    this.running = false;
    workerThread.interrupt();
    try {
      workerThread.join(3000);
    } catch (InterruptedException ignored) {
    }
    IOUtils.cleanup(LOG, out);
    LOG.info("{} close()", name);
  }

  /**
   * A snapshot has just been installed on the follower. Need to update the IO
   * worker's state accordingly.
   */
  void syncWithSnapshot(long lastSnapshotIndex) {
    queue.clear();
    lastWrittenIndex = lastSnapshotIndex;
    flushedIndex = lastSnapshotIndex;
    pendingFlushNum = 0;
  }

  @Override
  public String toString() {
    return name;
  }

  /**
   * This is protected by the RaftServer and RaftLog's lock.
   */
  private Task addIOTask(Task task) {
    LOG.debug("{} adds IO task {}", name, task);
    try {
      for(; !queue.offer(task, ONE_SECOND); ) {
        Preconditions.assertTrue(isAlive(),
            "the worker thread is not alive");
      }
    } catch (Throwable t) {
      if (t instanceof InterruptedException && !running) {
        LOG.info("Got InterruptedException when adding task " + task
            + ". The SegmentedRaftLogWorker already stopped.");
      } else {
        ExitUtils.terminate(2, "Failed to add IO task " + task, t, LOG);
      }
    }
    return task;
  }

  boolean isAlive() {
    return running && workerThread.isAlive();
  }

  @Override
  public void run() {
    while (running) {
      try {
        Task task = queue.poll(ONE_SECOND);
        if (task != null) {
          try {
            task.execute();
          } catch (IOException e) {
            if (task.getEndIndex() < lastWrittenIndex) {
              LOG.info("Ignore IOException when handling task " + task
                  + " which is smaller than the lastWrittenIndex."
                  + " There should be a snapshot installed.", e);
            } else {
              throw e;
            }
          }
          task.done();
        }
      } catch (InterruptedException e) {
        if (running) {
          LOG.warn("{} got interrupted while still running",
              Thread.currentThread().getName());
        }
        LOG.info(Thread.currentThread().getName()
            + " was interrupted, exiting. There are " + queue.getNumElements()
            + " tasks remaining in the queue.");
        Thread.currentThread().interrupt();
        return;
      } catch (Throwable t) {
        if (!running) {
          LOG.info("{} got closed and hit exception",
              Thread.currentThread().getName(), t);
        } else {
          // TODO avoid terminating the jvm, we should
          // 1) support multiple log directories
          // 2) only shutdown the raft server impl
          ExitUtils.terminate(1, Thread.currentThread().getName() + " failed.",
              t, LOG);
        }
      }
    }
  }

  private boolean shouldFlush() {
    return pendingFlushNum >= forceSyncNum ||
        (pendingFlushNum > 0 && queue.isEmpty());
  }

  private void flushWrites() throws IOException {
    if (out != null) {
      LOG.debug("{}: flush {}", name, out);
      final Timer.Context timerContext = logFlushTimer.get().time();
      try {
        final CompletableFuture<Void> f = stateMachine != null ?
            stateMachine.flushStateMachineData(lastWrittenIndex) :
            CompletableFuture.completedFuture(null);
        if (stateMachineDataPolicy.isSync()) {
          stateMachineDataPolicy.getFromFuture(f, () -> this + "-flushStateMachineData");
        }
        out.flush();
        if (!stateMachineDataPolicy.isSync()) {
          IOUtils.getFromFuture(f, () -> this + "-flushStateMachineData");
        }
      } finally {
        timerContext.stop();
      }
      updateFlushedIndex();
    }
  }

  private void updateFlushedIndex() {
    LOG.debug("{}: updateFlushedIndex {} -> {}", name, flushedIndex, lastWrittenIndex);
    flushedIndex = lastWrittenIndex;
    pendingFlushNum = 0;
    Optional.ofNullable(submitUpdateCommitEvent).ifPresent(Runnable::run);
  }

  /**
   * The following several methods (startLogSegment, rollLogSegment,
   * writeLogEntry, and truncate) are only called by SegmentedRaftLog which is
   * protected by RaftServer's lock.
   *
   * Thus all the tasks are created and added sequentially.
   */
  void startLogSegment(long startIndex) {
    LOG.info("{}: Starting segment from index:{}", name, startIndex);
    addIOTask(new StartLogSegment(startIndex));
  }

  void rollLogSegment(LogSegment segmentToClose) {
    LOG.info("{}: Rolling segment {} to index:{}", name,
        segmentToClose.toString(), segmentToClose.getEndIndex());
    addIOTask(new FinalizeLogSegment(segmentToClose));
    addIOTask(new StartLogSegment(segmentToClose.getEndIndex() + 1));
  }

  Task writeLogEntry(LogEntryProto entry) {
    return addIOTask(new WriteLog(entry));
  }

  Task truncate(TruncationSegments ts, long index) {
    LOG.info("{}: Truncating segments {}, start index {}", name, ts, index);
    return addIOTask(new TruncateLog(ts, index));
  }

  Task purge(TruncationSegments ts) {
    return addIOTask(new PurgeLog(ts, storage));
  }

  private static final class PurgeLog extends Task {
    private final TruncationSegments segments;
    private final RaftStorage storage;

    private PurgeLog(TruncationSegments segments, RaftStorage storage) {
      this.segments = segments;
      this.storage = storage;
    }

    @Override
    void execute() throws IOException {
      if (segments.toDelete != null) {
        for (SegmentFileInfo fileInfo : segments.toDelete) {
          File delFile = storage.getStorageDir()
                  .getClosedLogFile(fileInfo.startIndex, fileInfo.endIndex);
          FileUtils.deleteFile(delFile);
        }
      }
    }

    @Override
    long getEndIndex() {
      return 0;
    }
  }

  private class WriteLog extends Task {
    private final LogEntryProto entry;
    private final CompletableFuture<?> stateMachineFuture;
    private final CompletableFuture<Long> combined;

    WriteLog(LogEntryProto entry) {
      this.entry = ServerProtoUtils.removeStateMachineData(entry);
      if (this.entry == entry || stateMachine == null) {
        this.stateMachineFuture = null;
      } else {
        try {
          // this.entry != entry iff the entry has state machine data
          this.stateMachineFuture = stateMachine.writeStateMachineData(entry);
        } catch (Throwable e) {
          LOG.error(name + ": writeStateMachineData failed for index " + entry.getIndex()
              + ", entry=" + ServerProtoUtils.toLogEntryString(entry), e);
          throw e;
        }
      }
      this.combined = stateMachineFuture == null? super.getFuture()
          : super.getFuture().thenCombine(stateMachineFuture, (index, stateMachineResult) -> index);
    }

    @Override
    int getSerializedSize() {
      return ServerProtoUtils.getSerializedSize(entry);
    }

    @Override
    CompletableFuture<Long> getFuture() {
      return combined;
    }

    @Override
    public void execute() throws IOException {
      if (stateMachineDataPolicy.isSync() && stateMachineFuture != null) {
        stateMachineDataPolicy.getFromFuture(stateMachineFuture, () -> this + "-writeStateMachineData");
      }

      Preconditions.assertTrue(out != null);
      Preconditions.assertTrue(lastWrittenIndex + 1 == entry.getIndex(),
          "lastWrittenIndex == %s, entry == %s", lastWrittenIndex, entry);
      out.write(entry);
      lastWrittenIndex = entry.getIndex();
      pendingFlushNum++;
      if (shouldFlush()) {
        flushWrites();
      }
    }

    @Override
    long getEndIndex() {
      return entry.getIndex();
    }

    @Override
    public String toString() {
      return super.toString() + ": " + ServerProtoUtils.toLogEntryString(entry);
    }
  }

  private class FinalizeLogSegment extends Task {
    private final long startIndex;
    private final long endIndex;

    FinalizeLogSegment(LogSegment segmentToClose) {
      Preconditions.assertTrue(segmentToClose != null, "Log segment to be rolled is null");
      this.startIndex = segmentToClose.getStartIndex();
      this.endIndex = segmentToClose.getEndIndex();
    }

    @Override
    public void execute() throws IOException {
      IOUtils.cleanup(LOG, out);
      out = null;

      File openFile = storage.getStorageDir().getOpenLogFile(startIndex);
      Preconditions.assertTrue(openFile.exists(),
          () -> name + ": File " + openFile + " to be rolled does not exist");
      if (endIndex - startIndex + 1 > 0) {
        // finalize the current open segment
        File dstFile = storage.getStorageDir().getClosedLogFile(startIndex, endIndex);
        Preconditions.assertTrue(!dstFile.exists());

        FileUtils.move(openFile, dstFile);
        LOG.info("{}: Rolled log segment from {} to {}", name, openFile, dstFile);
      } else { // delete the file of the empty segment
        FileUtils.deleteFile(openFile);
        LOG.info("{}: Deleted empty log segment {}", name, openFile);
      }
      updateFlushedIndex();
    }

    @Override
    long getEndIndex() {
      return endIndex;
    }

    @Override
    public String toString() {
      return super.toString() + ": " + "startIndex=" + startIndex + " endIndex=" + endIndex;
    }
  }

  private class StartLogSegment extends Task {
    private final long newStartIndex;

    StartLogSegment(long newStartIndex) {
      this.newStartIndex = newStartIndex;
    }

    @Override
    void execute() throws IOException {
      File openFile = storage.getStorageDir().getOpenLogFile(newStartIndex);
      Preconditions.assertTrue(!openFile.exists(), "open file %s exists for %s",
          openFile, name);
      Preconditions.assertTrue(out == null && pendingFlushNum == 0);
      out = new SegmentedRaftLogOutputStream(openFile, false, segmentMaxSize,
          preallocatedSize, bufferSize);
      Preconditions.assertTrue(openFile.exists(), "Failed to create file %s for %s",
          openFile.getAbsolutePath(), name);
      LOG.info("{}: created new log segment {}", name, openFile);
    }

    @Override
    long getEndIndex() {
      return newStartIndex;
    }
  }

  private class TruncateLog extends Task {
    private final TruncationSegments segments;
    private final long truncateIndex;

    TruncateLog(TruncationSegments ts, long index) {
      this.segments = ts;
      this.truncateIndex = index;

    }

    @Override
    void execute() throws IOException {
      IOUtils.cleanup(null, out);
      out = null;
      CompletableFuture<Void> stateMachineFuture = null;
      if (stateMachine != null) {
        stateMachineFuture = stateMachine.truncateStateMachineData(truncateIndex);
      }

      if (segments.toTruncate != null) {
        File fileToTruncate = segments.toTruncate.isOpen ?
            storage.getStorageDir().getOpenLogFile(
                segments.toTruncate.startIndex) :
            storage.getStorageDir().getClosedLogFile(
                segments.toTruncate.startIndex,
                segments.toTruncate.endIndex);
        Preconditions.assertTrue(fileToTruncate.exists(),
            "File %s to be truncated does not exist", fileToTruncate);
        FileUtils.truncateFile(fileToTruncate, segments.toTruncate.targetLength);

        // rename the file
        File dstFile = storage.getStorageDir().getClosedLogFile(
            segments.toTruncate.startIndex, segments.toTruncate.newEndIndex);
        Preconditions.assertTrue(!dstFile.exists(),
            "Truncated file %s already exists ", dstFile);
        FileUtils.move(fileToTruncate, dstFile);
        LOG.info("{}: Truncated log file {} to length {} and moved it to {}", name,
            fileToTruncate, segments.toTruncate.targetLength, dstFile);

        // update lastWrittenIndex
        lastWrittenIndex = segments.toTruncate.newEndIndex;
      }
      if (segments.toDelete != null && segments.toDelete.length > 0) {
        long minStart = segments.toDelete[0].startIndex;
        for (SegmentFileInfo del : segments.toDelete) {
          final File delFile;
          if (del.isOpen) {
            delFile = storage.getStorageDir().getOpenLogFile(del.startIndex);
          } else {
            delFile = storage.getStorageDir()
                .getClosedLogFile(del.startIndex, del.endIndex);
          }
          Preconditions.assertTrue(delFile.exists(),
              "File %s to be deleted does not exist", delFile);
          FileUtils.deleteFile(delFile);
          LOG.info("{}: Deleted log file {}", name, delFile);
          minStart = Math.min(minStart, del.startIndex);
        }
        if (segments.toTruncate == null) {
          lastWrittenIndex = minStart - 1;
        }
      }
      if (stateMachineFuture != null) {
        IOUtils.getFromFuture(stateMachineFuture, () -> this + "-truncateStateMachineData");
      }
      updateFlushedIndex();
    }

    @Override
    long getEndIndex() {
      if (segments.toTruncate != null) {
        return segments.toTruncate.newEndIndex;
      } else if (segments.toDelete.length > 0) {
        return segments.toDelete[segments.toDelete.length - 1].endIndex;
      }
      return RaftServerConstants.INVALID_LOG_INDEX;
    }

    @Override
    public String toString() {
      return super.toString() + ": " + segments;
    }
  }

  long getFlushedIndex() {
    return flushedIndex;
  }
}
