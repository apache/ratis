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
package org.apache.ratis.statemachine;

import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.util.Preconditions;

import java.util.Objects;

import static org.apache.ratis.server.impl.RaftServerConstants.INVALID_LOG_INDEX;

/**
 * Tracks the term index that is applied to the StateMachine for simple state machines with
 * no concurrent snapshoting capabilities.
 */
class TermIndexTracker {
  static final TermIndex INIT_TERMINDEX =
      TermIndex.newTermIndex(INVALID_LOG_INDEX, INVALID_LOG_INDEX);

  private TermIndex current = INIT_TERMINDEX;

  //TODO: developer note: everything is synchronized for now for convenience.

  /**
   * Initialize the tracker with a term index (likely from a snapshot).
   */
  public synchronized void init(TermIndex termIndex) {
    this.current = termIndex;
  }

  public synchronized void reset() {
    init(INIT_TERMINDEX);
  }

  /**
   * Update the tracker with a new TermIndex. It means that the StateMachine has
   * this index in memory.
   */
  public synchronized void update(TermIndex termIndex) {
    Objects.requireNonNull(termIndex);
    Preconditions.assertTrue(termIndex.compareTo(current) >= 0);
    this.current = termIndex;
  }

  /**
   * Return latest term and index that is inserted to this tracker as an atomic
   * entity.
   */
  public synchronized TermIndex getLatestTermIndex() {
    return current;
  }

}
