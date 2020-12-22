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
package org.apache.ratis.server.raftlog;

import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.LogEntryProto.LogEntryBodyCase;
import org.apache.ratis.server.protocol.TermIndex;

import java.util.Comparator;

/** The header of a {@link LogEntryProto} including {@link TermIndex} and {@link LogEntryBodyCase}. */
public interface LogEntryHeader extends Comparable<LogEntryHeader> {
  LogEntryHeader[] EMPTY_ARRAY = {};

  /** @return the {@link TermIndex}. */
  TermIndex getTermIndex();

  default long getTerm() {
    return getTermIndex().getTerm();
  }

  default long getIndex() {
    return getTermIndex().getIndex();
  }

  /** @return the {@link LogEntryBodyCase}. */
  LogEntryBodyCase getLogEntryBodyCase();

  static LogEntryHeader valueOf(LogEntryProto entry) {
    return valueOf(TermIndex.valueOf(entry), entry.getLogEntryBodyCase());
  }

  static LogEntryHeader valueOf(TermIndex ti, LogEntryBodyCase logEntryBodyCase) {
    return new LogEntryHeader() {
      @Override
      public TermIndex getTermIndex() {
        return ti;
      }

      @Override
      public LogEntryBodyCase getLogEntryBodyCase() {
        return logEntryBodyCase;
      }

      @Override
      public int compareTo(LogEntryHeader that) {
        return Comparator.comparing(LogEntryHeader::getTermIndex)
            .thenComparing(LogEntryHeader::getLogEntryBodyCase)
            .compare(this, that);
      }

      @Override
      public int hashCode() {
        return ti.hashCode();
      }

      @Override
      public boolean equals(Object obj) {
        if (obj == this) {
          return true;
        } else if (!(obj instanceof LogEntryHeader)) {
          return false;
        }

        final LogEntryHeader that = (LogEntryHeader) obj;
        return this.getLogEntryBodyCase() == that.getLogEntryBodyCase()
            && this.getTermIndex().equals(that.getTermIndex());
      }
    };
  }
}
