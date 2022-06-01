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

import static org.apache.ratis.server.metrics.SegmentedRaftLogMetrics.RAFT_LOG_FLUSH_TIME;
import static org.apache.ratis.server.metrics.SegmentedRaftLogMetrics.RATIS_LOG_WORKER_METRICS;

import org.apache.ratis.metrics.RatisMetrics;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.LogProtoUtils;
import org.apache.ratis.server.raftlog.RaftLogBase;
import org.apache.ratis.server.raftlog.RaftLogIOException;
import org.apache.ratis.util.AutoCloseableLock;

import java.io.File;
import java.io.IOException;
import java.util.function.Consumer;

public interface RaftStorageTestUtils {
  static RaftStorage newRaftStorage(File dir) throws IOException {
    return new RaftStorageImpl(dir, null);
  }

  static String getLogFlushTimeMetric(String memberId) {
    return getRaftLogFullMetric(memberId, RAFT_LOG_FLUSH_TIME);
  }

  static String getRaftLogFullMetric(String memberId, String metricName) {
    return RatisMetrics.RATIS_APPLICATION_NAME_METRICS + "." + RATIS_LOG_WORKER_METRICS
        + "." + memberId + "." + metricName;
  }

  static void printLog(RaftLogBase log, Consumer<String> println) {
    if (log == null) {
      println.accept("log == null");
      return;
    }

    final TermIndex last;
    final long flushed, committed;
    try(AutoCloseableLock readlock = log.readLock()) {
      last = log.getLastEntryTermIndex();
      flushed = log.getFlushIndex();
      committed = log.getLastCommittedIndex();
    }
    final StringBuilder b = new StringBuilder();
    for(long i = 0; i <= last.getIndex(); i++) {
      b.setLength(0);
      b.append(i == flushed? 'f': ' ');
      b.append(i == committed? 'c': ' ');
      b.append(String.format("%3d: ", i));
      try {
        b.append(LogProtoUtils.toLogEntryString(log.get(i)));
      } catch (RaftLogIOException e) {
        b.append(e);
      }
      println.accept(b.toString());
    }
  }
}
