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

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.metrics.RatisMetricsRegistry;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.impl.RaftServerConstants;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.impl.ServerProtoUtils;
import org.apache.ratis.server.storage.RaftLogCache.SegmentFileInfo;
import org.apache.ratis.server.storage.RaftLogCache.TruncationSegments;
import org.apache.ratis.server.storage.SegmentedRaftLog.Task;
import org.apache.ratis.shaded.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * This class takes the responsibility of all the raft log related I/O ops for a
 * raft peer.
 */
class RaftLogWorker implements Runnable {
  static final Logger LOG = LoggerFactory.getLogger(RaftLogWorker.class);

  private final String name;
  /**
   * The task queue accessed by rpc handler threads and the io worker thread.
   */
  private final BlockingQueue<Task> queue = new ArrayBlockingQueue<>(4096);
  private volatile boolean running = true;
  private final Thread workerThread;

  private final RaftStorage storage;
  private volatile LogOutputStream out;
  private final RaftServerImpl raftServer;
  private final StateMachine stateMachine;
  private final Supplier<Timer> logFlushTimer;

  /**
   * The number of entries that have been written into the LogOutputStream but
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

  RaftLogWorker(RaftPeerId selfId, RaftServerImpl raftServer, RaftStorage storage,
                RaftProperties properties) {
    this.name = selfId + "-" + getClass().getSimpleName();
    LOG.info("new {} for {}", name, storage);

    this.raftServer = raftServer;
    this.stateMachine = raftServer != null? raftServer.getStateMachine(): null;

    this.storage = storage;
    this.segmentMaxSize =
        RaftServerConfigKeys.Log.segmentSizeMax(properties).getSize();
    this.preallocatedSize =
        RaftServerConfigKeys.Log.preallocatedSize(properties).getSize();
    this.bufferSize =
        RaftServerConfigKeys.Log.writeBufferSize(properties).getSizeInt();
    this.forceSyncNum = RaftServerConfigKeys.Log.forceSyncNum(properties);
    this.workerThread = new Thread(this, name);

    // Server Id can be null in unit tests
    Supplier<String> serverId = () -> raftServer == null || raftServer.getId() == null
        ? "null" : raftServer.getId().toString();
    this.logFlushTimer = JavaUtils.memoize(() -> RatisMetricsRegistry.getRegistry()
        .timer(MetricRegistry.name(RaftLogWorker.class, serverId.get(),
            "flush-time")));
  }

  void start(long latestIndex, File openSegmentFile) throws IOException {
    LOG.trace("{} start(latestIndex={}, openSegmentFile={})", name, latestIndex, openSegmentFile);
    lastWrittenIndex = latestIndex;
    flushedIndex = latestIndex;
    if (openSegmentFile != null) {
      Preconditions.assertTrue(openSegmentFile.exists());
      out = new LogOutputStream(openSegmentFile, true, segmentMaxSize,
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
      if (!queue.offer(task, 1, TimeUnit.SECONDS)) {
        Preconditions.assertTrue(isAlive(),
            "the worker thread is not alive");
        queue.put(task);
      }
    } catch (Throwable t) {
      if (t instanceof InterruptedException && !running) {
        LOG.info("Got InterruptedException when adding task " + task
            + ". The RaftLogWorker already stopped.");
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
        Task task = queue.poll(1, TimeUnit.SECONDS);
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
            + " was interrupted, exiting. There are " + queue.size()
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
      LOG.debug("flush data to " + out + ", reset pending_sync_number to 0");
      final Timer.Context timerContext = logFlushTimer.get().time();
      try {
        out.flush();
      } finally {
        timerContext.stop();
      }
      updateFlushedIndex();
    }
  }

  private void updateFlushedIndex() {
    flushedIndex = lastWrittenIndex;
    pendingFlushNum = 0;
    if (raftServer != null) {
      raftServer.submitLocalSyncEvent();
    }
  }

  /**
   * The following several methods (startLogSegment, rollLogSegment,
   * writeLogEntry, and truncate) are only called by SegmentedRaftLog which is
   * protected by RaftServer's lock.
   *
   * Thus all the tasks are created and added sequentially.
   */
  void startLogSegment(long startIndex) {
    addIOTask(new StartLogSegment(startIndex));
  }

  void rollLogSegment(LogSegment segmentToClose) {
    LOG.info("Rolling segment:{} index to:{}", name,
        (segmentToClose.getEndIndex() + 1));
    addIOTask(new FinalizeLogSegment(segmentToClose));
    addIOTask(new StartLogSegment(segmentToClose.getEndIndex() + 1));
  }

  Task writeLogEntry(LogEntryProto entry) {
    return addIOTask(new WriteLog(entry));
  }

  Task truncate(TruncationSegments ts) {
    return addIOTask(new TruncateLog(ts));
  }

  private class WriteLog extends Task {
    private final LogEntryProto entry;
    private final CompletableFuture<?> stateMachineFuture;
    private final CompletableFuture<Long> combined;

    WriteLog(LogEntryProto entry) {
      this.entry = ProtoUtils.removeStateMachineData(entry);
      if (this.entry == entry || stateMachine == null) {
        this.stateMachineFuture = null;
      } else {
        // this.entry != entry iff the entry has state machine data
        this.stateMachineFuture = stateMachine.writeStateMachineData(entry);
      }
      this.combined = stateMachineFuture == null? super.getFuture()
          : super.getFuture().thenCombine(stateMachineFuture, (index, stateMachineResult) -> index);
    }

    @Override
    CompletableFuture<Long> getFuture() {
      return combined;
    }

    @Override
    public void execute() throws IOException {
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
    private final LogSegment segmentToClose;

    FinalizeLogSegment(LogSegment segmentToClose) {
      this.segmentToClose = segmentToClose;
    }

    @Override
    public void execute() throws IOException {
      IOUtils.cleanup(LOG, out);
      out = null;
      Preconditions.assertTrue(segmentToClose != null);

      File openFile = storage.getStorageDir()
          .getOpenLogFile(segmentToClose.getStartIndex());
      LOG.debug("{} finalizing log segment {}", name, openFile);
      Preconditions.assertTrue(openFile.exists(),
          () -> name + ": File " + openFile + " does not exist, segmentToClose="
              + segmentToClose.toDebugString());
      if (segmentToClose.numOfEntries() > 0) {
        // finalize the current open segment
        File dstFile = storage.getStorageDir().getClosedLogFile(
            segmentToClose.getStartIndex(), segmentToClose.getEndIndex());
        Preconditions.assertTrue(!dstFile.exists());

        FileUtils.move(openFile, dstFile);
      } else { // delete the file of the empty segment
        FileUtils.deleteFile(openFile);
      }
      updateFlushedIndex();
    }

    @Override
    long getEndIndex() {
      return segmentToClose.getEndIndex();
    }

    @Override
    public String toString() {
      return super.toString() + ": " + segmentToClose.toDebugString();
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
      LOG.debug("{} creating new log segment {}", name, openFile);
      Preconditions.assertTrue(!openFile.exists(), "open file %s exists for %s",
          openFile, name);
      Preconditions.assertTrue(out == null && pendingFlushNum == 0);
      out = new LogOutputStream(openFile, false, segmentMaxSize,
          preallocatedSize, bufferSize);
      Preconditions.assertTrue(openFile.exists(), "Failed to create file %s for %s",
          openFile.getAbsolutePath(), name);
    }

    @Override
    long getEndIndex() {
      return newStartIndex;
    }
  }

  private class TruncateLog extends Task {
    private final TruncationSegments segments;

    TruncateLog(TruncationSegments ts) {
      this.segments = ts;
    }

    @Override
    void execute() throws IOException {
      IOUtils.cleanup(null, out);
      out = null;
      if (segments.toTruncate != null) {
        File fileToTruncate = segments.toTruncate.isOpen ?
            storage.getStorageDir().getOpenLogFile(
                segments.toTruncate.startIndex) :
            storage.getStorageDir().getClosedLogFile(
                segments.toTruncate.startIndex,
                segments.toTruncate.endIndex);
        FileUtils.truncateFile(fileToTruncate, segments.toTruncate.targetLength);

        // rename the file
        File dstFile = storage.getStorageDir().getClosedLogFile(
            segments.toTruncate.startIndex, segments.toTruncate.newEndIndex);
        Preconditions.assertTrue(!dstFile.exists());
        FileUtils.move(fileToTruncate, dstFile);

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
          FileUtils.deleteFile(delFile);
          minStart = Math.min(minStart, del.startIndex);
        }
        if (segments.toTruncate == null) {
          lastWrittenIndex = minStart - 1;
        }
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
