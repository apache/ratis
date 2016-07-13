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
package org.apache.hadoop.raft.server.storage;

import com.google.common.base.Preconditions;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.raft.proto.RaftProtos.LogEntryProto;
import org.apache.hadoop.raft.server.storage.LogSegment.SegmentFileInfo;
import org.apache.hadoop.raft.server.storage.RaftLogCache.TruncationSegments;
import org.apache.hadoop.raft.util.RaftUtils;
import org.apache.hadoop.util.ExitUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * This class takes the responsibility of all the raft log related I/O ops for a
 * raft peer. It provides both the synchronous (for follower) and asynchronous
 * (for leader) WRITE APIs.
 */
class RaftLogWorker implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(RaftLogWorker.class);
  /**
   * The task queue accessed by rpc handler threads and the io worker thread.
   */
  private final BlockingQueue<Task> queue = new ArrayBlockingQueue<>(4096);
  private volatile boolean running = true;
  private final Thread workerThread;

  private final RaftStorage storage;
  private LogOutputStream out;

  RaftLogWorker(RaftStorage storage) {
    this.storage = storage;
    workerThread = new Thread(this, this.getClass().getSimpleName());
  }

  public void start() {
    workerThread.start();
  }

  public void close() {
    this.running = false;
    workerThread.interrupt();
  }

  /**
   * should be protected by the RaftServer's lock
   */
  private void addIOTask(Task task) {
    LOG.debug("add task {}", task);
    try {
      if (!queue.offer(task, 1, TimeUnit.SECONDS)) {
        Preconditions.checkState(isThreadAlive(),
            "the worker thread is not alive");
        queue.put(task);
      }
    } catch (Throwable t) {
      terminate(t);
    }
  }

  private boolean isThreadAlive() {
    return running && workerThread.isAlive();
  }

  private void terminate(Throwable t) {
    String message = "Exception while handling raft log: " + t.getMessage();
    LOG.error(message, t);
    ExitUtil.terminate(1, message);
  }

  @Override
  public void run() {
    while (running) {
      try {
        Task task = queue.poll(1, TimeUnit.SECONDS);
        if (task != null) {
          task.execute();
        }
      } catch (InterruptedException e) {
        LOG.info(Thread.currentThread().getName() + " was interrupted, exiting");
      } catch (Throwable t) {
        // TODO avoid terminating the jvm by supporting multiple log directories
        terminate(t);
      }
    }
  }

  void startLogSegment(long startIndex) {
    addIOTask(new StartLogSegment(startIndex));
  }

  void rollLogSegment(LogSegment segmentToClose) {
    addIOTask(new FinalizeLogSegment(segmentToClose));
    addIOTask(new StartLogSegment(segmentToClose.getEndIndex() + 1));
  }

  void writeLogEntry(LogEntryProto entry) {
    addIOTask(new WriteLog(entry));
  }

  void truncate(TruncationSegments ts) {
    addIOTask(new TruncateLog(ts));
  }

  /**
   * I/O task definitions.
   */
  private interface Task {
    void execute() throws IOException;
  }

  private class WriteLog implements Task {
    private final LogEntryProto entry;

    WriteLog(LogEntryProto entry) {
      this.entry = entry;
    }

    @Override
    public void execute() throws IOException {
      Preconditions.checkState(out != null);
      out.write(entry);
      // TODO when to flush?
    }
  }

  private class FinalizeLogSegment implements Task {
    private final LogSegment segmentToClose;

    FinalizeLogSegment(LogSegment segmentToClose) {
      this.segmentToClose = segmentToClose;
    }

    @Override
    public void execute() throws IOException {
      IOUtils.cleanup(null, out);
      out = null;
      Preconditions.checkState(segmentToClose != null);

      File openFile = storage.getStorageDir()
          .getOpenLogFile(segmentToClose.getStartIndex());
      Preconditions.checkState(openFile.exists());
      if (segmentToClose.numOfEntries() > 0) {
        // finalize the current open segment
        File dstFile = storage.getStorageDir().getClosedLogFile(
            segmentToClose.getStartIndex(), segmentToClose.getEndIndex());
        Preconditions.checkState(!dstFile.exists());

        NativeIO.renameTo(openFile, dstFile);
      } else { // delete the file of the empty segment
        deleteFile(openFile);
      }
    }
  }

  private void deleteFile(File f) throws IOException {
    try {
      Files.delete(f.toPath());
    } catch (IOException e) {
      LOG.warn("Could not delete " + f);
      throw e;
    }
  }

  private class StartLogSegment implements Task {
    private final long newStartIndex;

    StartLogSegment(long newStartIndex) {
      this.newStartIndex = newStartIndex;
    }

    @Override
    public void execute() throws IOException {
      File openFile = storage.getStorageDir().getOpenLogFile(newStartIndex);
      Preconditions.checkState(!openFile.exists());
      Preconditions.checkState(out == null);
      out = new LogOutputStream(openFile, false);
    }
  }

  private class TruncateLog implements Task {
    private final TruncationSegments segments;

    TruncateLog(TruncationSegments ts) {
      this.segments = ts;
    }

    @Override
    public void execute() throws IOException {
      IOUtils.cleanup(null, out);
      out = null;
      if (segments.toTruncate != null) {
        File fileToTruncate = segments.toTruncate.isOpen ?
            storage.getStorageDir().getOpenLogFile(
                segments.toTruncate.startIndex) :
            storage.getStorageDir().getClosedLogFile(
                segments.toTruncate.startIndex,
                segments.toTruncate.endIndex);
        RaftUtils.truncateFile(fileToTruncate, segments.toTruncate.targetLength);

        // rename the file
        File dstFile = storage.getStorageDir().getClosedLogFile(
            segments.toTruncate.startIndex, segments.toTruncate.newEndIndex);
        Preconditions.checkState(!dstFile.exists());
        NativeIO.renameTo(fileToTruncate, dstFile);
      }
      if (segments.toDelete != null) {
        for (SegmentFileInfo del : segments.toDelete) {
          final File delFile;
          if (del.isOpen) {
            delFile = storage.getStorageDir().getOpenLogFile(del.startIndex);
          } else {
            delFile = storage.getStorageDir()
                .getClosedLogFile(del.startIndex, del.endIndex);
          }
          deleteFile(delFile);
        }
      }
    }
  }
}
