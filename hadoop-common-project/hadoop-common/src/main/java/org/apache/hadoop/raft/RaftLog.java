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

import com.google.common.base.Preconditions;

class RaftLog {
  static class TermIndex implements Comparable<TermIndex> {
    private final long term;
    private final long index; //log index; first index is 1.

    TermIndex(long term, long logIndex) {
      this.term = term;
      this.index = logIndex;
    }

    long getTerm() {
      return term;
    }

    long getIndex() {
      return index;
    }

    @Override
    public int compareTo(TermIndex that) {
      final int diff = Long.compare(this.term, that.term);
      return diff != 0? diff: Long.compare(this.index, that.index);
    }

    static boolean equal(TermIndex a, TermIndex b) {
      return a == b || (a != null && b != null && a.compareTo(b) == 0);
    }
  }

  static interface Message {

  }

  static abstract class Entry extends TermIndex {
    static void assertEntries(long expectedTerm, Entry... entries) {
      if (entries.length > 0) {
        final long index0 = entries[0].getIndex();
        for(int i = 0; i < entries.length; i++) {
          final long t = entries[i].getTerm();
          Preconditions.checkArgument(expectedTerm == t,
              "Unexpected Term: entries[{}].getTerm()={} but expectedTerm={}",
              i, t, expectedTerm);

          final long indexi = entries[i].getIndex();
          Preconditions.checkArgument(indexi == index0 + i, "Unexpected Index: "
              + "entries[{}].getIndex()={} but entries[0].getIndex()={}",
              i, indexi, index0);
        }
      }
    }

    private final Message message;

    Entry(long term, long logIndex, Message message) {
      super(term, logIndex);
      this.message = message;
    }
  }

  private List<Entry> entries;
  private int lastCommitted = -1;

  synchronized TermIndex getLastCommitted() {
    return lastCommitted > 0? entries.get(lastCommitted): new TermIndex(-1, -1);
  }

  synchronized TermIndex get(long index) {
    //TODO
    return null;
  }

  /** Does the contain the given term and index? */
  boolean contains(TermIndex ti) {
    return TermIndex.equal(ti, get(ti.getIndex()));
  }

  synchronized Entry getLastEntry() {
    final int size = entries.size();
    return size == 0? null: entries.get(size - 1);
  }

  long getNextIndex() {
    final Entry last = getLastEntry();
    return last == null? 1: last.getIndex() + 1;
  }

  /**
   * If an existing entry conflicts with a new one (same index but different
   * terms), delete the existing entry and all that follow it (§5.3)
   */
  synchronized void check(TermIndex ti) {


  }
}
