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
import org.apache.ratis.thirdparty.com.google.common.cache.Cache;
import org.apache.ratis.thirdparty.com.google.common.cache.CacheBuilder;

import java.util.Comparator;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/** The term and the log index defined in the Raft consensus algorithm. */
public interface TermIndex extends Comparable<TermIndex> {
  class Util {
    /** An LRU Cache for {@link TermIndex} instances */
    private static final Cache<TermIndex, TermIndex> CACHE = CacheBuilder.newBuilder()
          .maximumSize(1 << 16)
          .expireAfterAccess(1, TimeUnit.MINUTES)
          .build();
  }
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
    final TermIndex key = new TermIndex() {
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
        return n >= 0L? String.valueOf(n) : "~";
      }

      @Override
      public String toString() {
        return String.format("(t:%s, i:%s)", longToString(term), longToString(index));
      }
    };
    try {
      return Util.CACHE.get(key, () -> key);
    } catch (ExecutionException e) {
      throw new IllegalStateException("Failed to valueOf(" + term + ", " + index + "), key=" + key, e);
    }
  }
}