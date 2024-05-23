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

public class TermIndexImpl implements TermIndex {
  private final long term;
  private final long index;

  public TermIndexImpl(long term, long index) {
    this.term = term;
    this.index = index;
  }

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
}
