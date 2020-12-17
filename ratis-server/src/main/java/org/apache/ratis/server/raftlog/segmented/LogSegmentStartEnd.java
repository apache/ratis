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

import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.util.Preconditions;

import java.io.File;
import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * The start index and an end index of a log segment.
 *
 * This is a value-based class.
 */
public final class LogSegmentStartEnd implements Comparable<LogSegmentStartEnd> {
  private static final String LOG_FILE_NAME_PREFIX = "log";
  private static final String IN_PROGRESS = "inprogress";
  private static final Pattern CLOSED_SEGMENT_PATTERN;
  private static final Pattern OPEN_SEGMENT_PATTERN;

  static {
    final String digits = "(\\d+)";
    CLOSED_SEGMENT_PATTERN = Pattern.compile(LOG_FILE_NAME_PREFIX + "_" + digits + "-" + digits);
    OPEN_SEGMENT_PATTERN = Pattern.compile(LOG_FILE_NAME_PREFIX + "_" + IN_PROGRESS + "_" + digits + "(?:\\..*)?");
  }

  private static String getOpenLogFileName(long startIndex) {
    return LOG_FILE_NAME_PREFIX + "_" + IN_PROGRESS + "_" + startIndex;
  }

  static Pattern getOpenSegmentPattern() {
    return OPEN_SEGMENT_PATTERN;
  }

  private static String getClosedLogFileName(long startIndex, long endIndex) {
    return LOG_FILE_NAME_PREFIX + "_" + startIndex + "-" + endIndex;
  }

  static Pattern getClosedSegmentPattern() {
    return CLOSED_SEGMENT_PATTERN;
  }

  static LogSegmentStartEnd valueOf(long startIndex) {
    return new LogSegmentStartEnd(startIndex, null);
  }

  static LogSegmentStartEnd valueOf(long startIndex, Long endIndex) {
    return new LogSegmentStartEnd(startIndex, endIndex);
  }

  static LogSegmentStartEnd valueOf(long startIndex, long endIndex, boolean isOpen) {
    return new LogSegmentStartEnd(startIndex, isOpen? null: endIndex);
  }

  private final long startIndex;
  private final Long endIndex;

  private LogSegmentStartEnd(long startIndex, Long endIndex) {
    Preconditions.assertTrue(startIndex >= RaftLog.LEAST_VALID_LOG_INDEX);
    Preconditions.assertTrue(endIndex == null || endIndex >= startIndex);
    this.startIndex = startIndex;
    this.endIndex = endIndex;
  }

  public long getStartIndex() {
    return startIndex;
  }

  public long getEndIndex() {
    return Optional.ofNullable(endIndex).orElse(RaftLog.INVALID_LOG_INDEX);
  }

  public boolean isOpen() {
    return endIndex == null;
  }

  private String getFileName() {
    return isOpen()? getOpenLogFileName(startIndex): getClosedLogFileName(startIndex, endIndex);
  }

  File getFile(File dir) {
    return new File(dir, getFileName());
  }

  File getFile(RaftStorage storage) {
    return getFile(storage.getStorageDir().getCurrentDir());
  }

  @Override
  public int compareTo(LogSegmentStartEnd that) {
    return Comparator.comparingLong(LogSegmentStartEnd::getStartIndex)
        .thenComparingLong(LogSegmentStartEnd::getEndIndex)
        .compare(this, that);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final LogSegmentStartEnd that = (LogSegmentStartEnd) obj;
    return startIndex == that.startIndex && Objects.equals(endIndex, that.endIndex);
  }

  @Override
  public int hashCode() {
    return Objects.hash(startIndex, endIndex);
  }

  @Override
  public String toString() {
    return startIndex + "-" + Optional.ofNullable(endIndex).map(Object::toString).orElse("");
  }
}