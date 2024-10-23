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

import org.apache.ratis.server.RaftServer;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.Slf4jUtils;
import org.slf4j.event.Level;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

public interface SegmentedRaftLogTestUtils {
  SizeInBytes MAX_OP_SIZE = SizeInBytes.valueOf("32MB");

  static SegmentedRaftLogInputStream newSegmentedRaftLogInputStream(File log,
      long startIndex, long endIndex, boolean isOpen) {
    final LogSegmentStartEnd startEnd = LogSegmentStartEnd.valueOf(startIndex, endIndex, isOpen);
    return new SegmentedRaftLogInputStream(log, startEnd, MAX_OP_SIZE, null);
  }

  static void setRaftLogWorkerLogLevel(Level level) {
    Slf4jUtils.setLogLevel(SegmentedRaftLogWorker.LOG, level);
  }

  static List<Path> getOpenLogFiles(RaftServer.Division server) throws Exception {
    return LogSegmentPath.getLogSegmentPaths(server.getRaftStorage()).stream()
        .filter(p -> p.getStartEnd().isOpen())
        .map(LogSegmentPath::getPath)
        .collect(Collectors.toList());
  }
}
