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
import org.apache.hadoop.util.ExitUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * This class takes the responsibility of all the raft log related I/O ops for a
 * raft peer. It provides both the synchronous (for follower) and asynchronous
 * (for leader) WRITE APIs.
 */
public class RaftLogWorker implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(RaftLogWorker.class);
  /**
   * The task queue accessed by rpc handler threads and the io worker thread.
   */
  private final BlockingQueue<Task> queue = new ArrayBlockingQueue<>(4096);
  private volatile boolean running = true;
  private final Thread workerThread;

  public RaftLogWorker() {
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
    String message = "Exception while logging: " + t.getMessage();
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
        terminate(t);
      }
    }
  }

  /**
   * I/O task definitions.
   */
  private static abstract class Task {
    private final long sequenceNum;

    Task(long seqNum) {
      this.sequenceNum = seqNum;
    }

    abstract void execute();

    long getSequenceNum() {
      return sequenceNum;
    }

    @Override
    public String toString() {
      return this.getClass().getSimpleName() + "-" + getSequenceNum();
    }
  }

  private class WriteLog extends Task {
    private final LogSegment.LogRecord[] records;

    WriteLog(long seqNum, LogSegment.LogRecord[] records) {
      super(seqNum);
      this.records = records;
    }

    @Override
    void execute() {

    }
  }

  private class RollLogSegment extends Task {
    RollLogSegment(long seqNum) {
      super(seqNum);
    }

    @Override
    void execute() {

    }
  }

  private class TruncateLog extends Task {
    TruncateLog(long seqNum) {
      super(seqNum);
    }

    @Override
    void execute() {

    }
  }

  private class DeleteLogSegment extends Task {
    DeleteLogSegment(long seqNum) {
      super(seqNum);
    }

    @Override
    void execute() {

    }
  }

  private class UpdateMetadata extends Task {
    UpdateMetadata(long seqNum) {
      super(seqNum);
    }

    @Override
    void execute() {

    }
  }
}
