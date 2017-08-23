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
package org.apache.ratis.server.protocol;

import org.apache.ratis.server.impl.ServerImplUtils;

import java.util.function.LongFunction;

/** The term and the log index defined in the Raft consensus algorithm. */
public interface TermIndex extends Comparable<TermIndex> {
  TermIndex[] EMPTY_TERMINDEX_ARRAY = {};

  /** @return the term. */
  long getTerm();

  /** @return the index. */
  long getIndex();

  /** Create a new {@link TermIndex} instance. */
  static TermIndex newTermIndex(long term, long index) {
    return ServerImplUtils.newTermIndex(term, index);
  }

  LongFunction<String> LONG_TO_STRING = n -> n >= 0L? String.valueOf(n): "~";

  /** @return a string representing the given term and index. */
  static String toString(long term, long index) {
    return String.format("(t:%s, i:%s)",
        LONG_TO_STRING.apply(term), LONG_TO_STRING.apply(index));
  }
}


