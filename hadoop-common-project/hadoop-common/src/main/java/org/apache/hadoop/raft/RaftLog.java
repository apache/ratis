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
package org.apache.hadoop.raft;

import java.util.List;

class RaftLog {
  static class TermIndex implements Comparable<TermIndex> {
    final long term;
    final long logIndex;

    TermIndex(long term, long logIndex) {
      this.term = term;
      this.logIndex = logIndex;
    }

    @Override
    public int compareTo(TermIndex that) {
      final int diff = Long.compare(this.term, that.term);
      return diff != 0? diff: Long.compare(this.logIndex, that.logIndex);
    }
  }

  static abstract class Entry extends TermIndex {
    Entry(long term, long logIndex) {
      super(term, logIndex);
    }
  }

  private List<Entry> entries;
  private int lastCommitted = -1;

  TermIndex getLastCommitted() {
    return lastCommitted > 0? entries.get(lastCommitted): new TermIndex(-1, -1);
  }
}
