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
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/** The term and the log index defined in the Raft consensus algorithm. */
public interface TermIndex extends Comparable<TermIndex> {
  TermIndex[] EMPTY_ARRAY = {};

  /** Shared Guava Cache for TermIndex instances */
  Cache<TermIndexKey, TermIndex> CACHE = CacheBuilder.newBuilder()
      .maximumSize(10000)
      .expireAfterAccess(10, TimeUnit.MINUTES)
      .build();


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
    TermIndexKey key = new TermIndexKey(term, index);
    try {
      return CACHE.get(key, () -> new TermIndexImpl(term, index));
    } catch (ExecutionException e) {
      throw new IllegalStateException("Failed to valueOf(" + term + ", " + index + ")", e);
    }
  }

  class TermIndexKey {
    private final long term;
    private final long index;

    public TermIndexKey(long term, long index) {
      this.term = term;
      this.index = index;
    }

    public long getTerm() {
      return term;
    }

    public long getIndex() {
      return index;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TermIndexKey that = (TermIndexKey) o;
      return term == that.term && index == that.index;
    }

    @Override
    public int hashCode() {
      return Objects.hash(term, index);
    }
  }
}