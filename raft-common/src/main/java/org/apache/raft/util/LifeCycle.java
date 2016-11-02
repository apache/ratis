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
package org.apache.raft.util;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The life cycle of a machine.
 * <pre>
 *   -------------------------------------------------
 *  |           --------------------------            |
 *  |          |                          |           |
 *  |        PAUSED <------ PAUSING       |           |
 *  |          |            ^     |       |           |
 *  |          V            |     |       V           V
 * NEW --> STARTING --> RUNNING --|--> CLOSING --> [CLOSED]
 *             |            |     |       ^
 *             |            V     V       |
 *              ---------> EXCEPTION -----
 * </pre>
 * Note that there is no transition from PAUSING to CLOSING.
 */
public class LifeCycle {
  public static final Logger LOG = LoggerFactory.getLogger(LifeCycle.class);

  /** The states in the life cycle. */
  public enum State {
    /** The machine is newly created and holds zero resource. */
    NEW,
    /** The machine is starting and does not yet provide any service. */
    STARTING,
    /** The machine is running and providing service. */
    RUNNING,
    /** The machine is pausing and stopping providing service. */
    PAUSING,
    /** The machine is paused and does not provide any service. */
    PAUSED,
    /** The machine catches an internal exception so that it must be closed. */
    EXCEPTION,
    /** The machine is closing, stopping providing service and releasing resources. */
    CLOSING,
    /** The machine is closed, a final state. */
    CLOSED;

    private static final Map<State, List<State>> PREDECESSORS;

    static void put(State key, Map<State, List<State>> map, State... values) {
      map.put(key, Collections.unmodifiableList(Arrays.asList(values)));
    }

    static {
      final Map<State, List<State>> predecessors = new EnumMap<>(State.class);
      put(NEW, predecessors);
      put(STARTING,  predecessors, NEW, PAUSED);
      put(RUNNING,   predecessors, STARTING);
      put(PAUSING,   predecessors, RUNNING);
      put(PAUSED,    predecessors, PAUSING);
      put(EXCEPTION, predecessors, STARTING, PAUSING, RUNNING);
      put(CLOSING,   predecessors, RUNNING, PAUSED, EXCEPTION);
      put(CLOSED,    predecessors, NEW, CLOSING);

      PREDECESSORS = Collections.unmodifiableMap(predecessors);
    }

    /** Is the given transition valid? */
    static boolean isValid(State from, State to) {
      return PREDECESSORS.get(to).contains(from);
    }

    /** Validate the given transition. */
    static void validate(State from, State to) {
      Preconditions.checkState(isValid(from, to),
          "Illegal transition: %s -> %s", from, to);
    }
  }

  private final String name;
  private final AtomicReference<State> current = new AtomicReference<>(State.NEW);

  public LifeCycle(Object name) {
    this.name = name.toString();
  }

  /** Transition from the current state to the given state. */
  public void transition(final State to) {
    final State from = current.getAndSet(to);
    LOG.trace("{}: {} -> {}", name, from, to);
    State.validate(from, to);
  }

  /**
   * If the current state is equal to the specified from state,
   * then transition to the give to state; otherwise, make no change.
   *
   * @return true iff the current state is equal to the specified from state.
   */
  public boolean compareAndTransition(final State from, final State to) {
    if (current.compareAndSet(from, to)) {
      LOG.trace("{}: {} -> {}", name, from, to);
      State.validate(from, to);
      return true;
    }
    return false;
  }

  /** @return the current state. */
  public State getCurrentState() {
    return current.get();
  }
}
