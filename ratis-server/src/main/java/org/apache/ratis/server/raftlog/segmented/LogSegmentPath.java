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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;

/**
 * {@link LogSegmentStartEnd} with a {@link Path}.
 *
 * This is a value-based class.
 */
public final class LogSegmentPath implements Comparable<LogSegmentPath> {
  static final Logger LOG = LoggerFactory.getLogger(LogSegmentPath.class);

  private final Path path;
  private final LogSegmentStartEnd startEnd;

  private LogSegmentPath(Path path, long startIndex, Long endIndex) {
    this.path = path;
    this.startEnd = LogSegmentStartEnd.valueOf(startIndex, endIndex);
  }

  public Path getPath() {
    return path;
  }

  public LogSegmentStartEnd getStartEnd() {
    return startEnd;
  }

  @Override
  @SuppressFBWarnings("EQ_COMPARETO_USE_OBJECT_EQUALS")
  public int compareTo(LogSegmentPath that) {
    return Comparator.comparing(LogSegmentPath::getStartEnd).compare(this, that);
  }

  @Override
  public String toString() {
    return path+ "(" + startEnd + ")";
  }

  /**
   * Get a list of {@link LogSegmentPath} from the given storage.
   *
   * @return a list of log segment paths sorted by the indices.
   */
  public static List<LogSegmentPath> getLogSegmentPaths(RaftStorage storage) throws IOException {
    return getLogSegmentPaths(storage.getStorageDir().getCurrentDir().toPath());
  }

  private static List<LogSegmentPath> getLogSegmentPaths(Path dir) throws IOException {
    final List<LogSegmentPath> list = new ArrayList<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
      for (Path path : stream) {
        Optional.ofNullable(matchLogSegment(path)).ifPresent(list::add);
      }
    }
    list.sort(Comparator.naturalOrder());
    return list;
  }

  /**
   * Match the given path with the {@link LogSegmentStartEnd#getClosedSegmentPattern()}
   * or the {@link LogSegmentStartEnd#getOpenSegmentPattern()}.
   *
   * Note that if the path is a zero size open segment, this method will try to delete it.
   *
   * @return the log segment file matching the given path.
   */
  public static LogSegmentPath matchLogSegment(Path path) {
    return Optional.ofNullable(matchCloseSegment(path)).orElseGet(() -> matchOpenSegment(path));
  }

  @SuppressFBWarnings("NP_NULL_ON_SOME_PATH")
  private static LogSegmentPath matchCloseSegment(Path path) {
    final Matcher matcher = LogSegmentStartEnd.getClosedSegmentPattern().matcher(path.getFileName().toString());
    if (matcher.matches()) {
      Preconditions.assertTrue(matcher.groupCount() == 2);
      return newInstance(path, matcher.group(1), matcher.group(2));
    }
    return null;
  }

  @SuppressFBWarnings("NP_NULL_ON_SOME_PATH")
  private static LogSegmentPath matchOpenSegment(Path path) {
    final Matcher matcher = LogSegmentStartEnd.getOpenSegmentPattern().matcher(path.getFileName().toString());
    if (matcher.matches()) {
      if (path.toFile().length() > 0L) {
        return newInstance(path, matcher.group(1), null);
      }

      LOG.info("Found zero size open segment file " + path);
      try {
        Files.delete(path);
        LOG.info("Deleted zero size open segment file " + path);
      } catch (IOException e) {
        LOG.warn("Failed to delete zero size open segment file " + path + ": " + e);
      }
    }
    return null;
  }

  private static LogSegmentPath newInstance(Path path, String startIndexString, String endIndexString) {
    final long startIndex = Long.parseLong(startIndexString);
    final Long endIndex = Optional.ofNullable(endIndexString).map(Long::parseLong).orElse(null);
    return new LogSegmentPath(path, startIndex, endIndex);
  }
}