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
package org.apache.ratis.server.protocol;

import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.TermIndexProto;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.util.BiWeakValueCache;

import java.util.Comparator;
import java.util.Optional;

/** The term and the log index defined in the Raft consensus algorithm. */
public interface TermIndex extends Comparable<TermIndex> {
  /**
   * The initial value.
   * When a new Raft group starts,
   * all the servers has term 0 and index -1 (= {@link RaftLog#INVALID_LOG_INDEX}).
   * Note that term is incremented during leader election
   * and index is incremented when writing to the {@link RaftLog}.
   * The least term and index possibly written to the {@link RaftLog}
   * are respectively 1 and 0 (= {@link RaftLog#LEAST_VALID_LOG_INDEX}).
   */
  TermIndex INITIAL_VALUE = valueOf(0, RaftLog.INVALID_LOG_INDEX);

  /** An empty {@link TermIndex} array. */
  TermIndex[] EMPTY_ARRAY = {};

  /** @return the term. */
  long getTerm();

  /** @return the index. */
  long getIndex();

  /** @return the {@link TermIndexProto}. */
  default TermIndexProto toProto() {
    return TermIndexProto.newBuilder()
        .setTerm(getTerm())
        .setIndex(getIndex())
        .build();
  }

  @Override
  default int compareTo(TermIndex that) {
    return Comparator.comparingLong(TermIndex::getTerm)
        .thenComparingLong(TermIndex::getIndex)
        .compare(this, that);
  }

  /** @return a {@link TermIndex} object from the given proto. */
  static TermIndex valueOf(TermIndexProto proto) {
    return Optional.ofNullable(proto).map(p -> valueOf(p.getTerm(), p.getIndex())).orElse(null);
  }

  /** @return a {@link TermIndex} object from the given proto. */
  static TermIndex valueOf(LogEntryProto proto) {
    return Optional.ofNullable(proto).map(p -> valueOf(p.getTerm(), p.getIndex())).orElse(null);
  }

  /** @return a {@link TermIndex} object. */
  static TermIndex valueOf(long term, long index) {
    return Impl.getCache().getOrCreate(term, index);
  }

  /**
   * An implementation for private use.
   * Note that this is not a public API, although this is public class.
   */
  final class Impl {
    private Impl() { }

    private static final BiWeakValueCache<Long, Long, TermIndex> CACHE
        = new BiWeakValueCache<>("term", "index", Impl::newTermIndex);

    static BiWeakValueCache<Long, Long, TermIndex> getCache() {
      return CACHE;
    }

    private static TermIndex newTermIndex(long term, long index) {
      return new TermIndex() {
        @Override
        public long getTerm() {
          return term;
        }

        @Override
        public long getIndex() {
          return index;
        }

        @Override
        public boolean equals(Object obj) {
          if (obj == this) {
            return true;
          } else if (!(obj instanceof TermIndex)) {
            return false;
          }

          final TermIndex that = (TermIndex) obj;
          return this.getTerm() == that.getTerm()
              && this.getIndex() == that.getIndex();
        }

        @Override
        public int hashCode() {
          return Long.hashCode(term) ^ Long.hashCode(index);
        }

        private String longToString(long n) {
          return n >= 0L ? String.valueOf(n) : "~";
        }

        @Override
        public String toString() {
          return String.format("(t:%s, i:%s)", longToString(term), longToString(index));
        }
      };
    }
  }
}