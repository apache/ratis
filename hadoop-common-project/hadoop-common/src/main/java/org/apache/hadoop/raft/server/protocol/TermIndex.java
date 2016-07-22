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
package org.apache.hadoop.raft.server.protocol;

import org.apache.commons.lang.builder.HashCodeBuilder;

import java.util.Comparator;

public class TermIndex implements Comparable<TermIndex> {
  private final long term;
  private final long index; //log index; first index is 1.

  public TermIndex(long term, long logIndex) {
    this.term = term;
    this.index = logIndex;
  }

  public long getTerm() {
    return term;
  }

  public long getIndex() {
    return index;
  }

  @Override
  public int compareTo(TermIndex that) {
    final int diff = Long.compare(this.term, that.term);
    return diff != 0 ? diff : Long.compare(this.index, that.index);
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof TermIndex) {
      final TermIndex ti = (TermIndex) o;
      return this == ti ||
          (this.term == ti.getTerm() && this.index == ti.getIndex());
    }
    return false;
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(term).append(index).hashCode();
  }

  private static String toString(long n) {
    return n < 0 ? "~" : "" + n;
  }

  @Override
  public String toString() {
    return "t" + toString(term) + "i" + toString(index);
  }
}
