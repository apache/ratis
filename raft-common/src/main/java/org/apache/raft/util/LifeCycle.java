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
 *   ----------------------------------------------------
 *  |                                                    |
 *  |                                                    V
 * NEW --> STARTING --> RUNNING ----- --> CLOSING --> [CLOSED]
 *          |    ^          |        |       ^
 *          |    |          V        |       |
 *          |   PAUSED <-- PAUSING   |       |
 *          |                  |     |       |
 *          |                  V     V       |
 *           ---------------> EXCEPTION -----
 * </pre>
 */
public class LifeCycle {
  public static final Logger LOG = LoggerFactory.getLogger(LifeCycle.class);

  public enum State {
    NEW,
    STARTING,
    RUNNING,
    PAUSING,
    PAUSED,
    EXCEPTION,
    CLOSING,
    CLOSED;

    private static final Map<State, List<State>> PREDECESSORS;

    static {
      final Map<State, List<State>> m = new EnumMap<>(State.class);
      m.put(NEW, Collections.emptyList());
      m.put(STARTING, Collections.unmodifiableList(Arrays.asList(NEW, PAUSED)));
      m.put(RUNNING, Collections.unmodifiableList(Arrays.asList(STARTING)));
      m.put(PAUSING, Collections.unmodifiableList(Arrays.asList(RUNNING)));
      m.put(PAUSED, Collections.unmodifiableList(Arrays.asList(PAUSING)));
      m.put(EXCEPTION, Collections.unmodifiableList(Arrays.asList(STARTING, PAUSING, RUNNING)));
      m.put(CLOSING, Collections.unmodifiableList(Arrays.asList(RUNNING, EXCEPTION)));
      m.put(CLOSED, Collections.unmodifiableList(Arrays.asList(NEW, CLOSING)));

      PREDECESSORS = Collections.unmodifiableMap(m);
    }

    /** Validate the given transition. */
    static void validate(State from, State to) {
      Preconditions.checkState(PREDECESSORS.get(to).contains(from),
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
