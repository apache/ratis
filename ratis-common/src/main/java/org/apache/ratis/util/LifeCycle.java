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
package org.apache.ratis.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The life cycle of a machine.
 * <pre>
 *   -------------------------------------------------
 *  |        --------------------------------         |
 *  |       |     ------------------------   |        |
 *  |       |    |                        |  |        |
 *  |       |  PAUSED <---- PAUSING----   |  |        |
 *  |       |    |          ^     |    |  |  |        |
 *  |       |    V          |     |    V  V  V        V
 * NEW --> STARTING --> RUNNING --|--> CLOSING --> [CLOSED]
 *  ^       |    |          |     |       ^
 *  |       |    |          V     V       |
 *   -------      -------> EXCEPTION -----
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

    /** Does this object equal to one of the given states? */
    public boolean isOneOf(State... states) {
      for(State e : states) {
        if (e == this) {
          return true;
        }
      }
      return false;
    }

    static void put(State key, Map<State, List<State>> map, State... values) {
      map.put(key, Collections.unmodifiableList(Arrays.asList(values)));
    }

    static {
      final Map<State, List<State>> predecessors = new EnumMap<>(State.class);
      put(NEW,       predecessors, STARTING);
      put(STARTING,  predecessors, NEW, PAUSED);
      put(RUNNING,   predecessors, STARTING);
      put(PAUSING,   predecessors, RUNNING);
      put(PAUSED,    predecessors, PAUSING);
      put(EXCEPTION, predecessors, STARTING, PAUSING, RUNNING);
      put(CLOSING,   predecessors, STARTING, RUNNING, PAUSING, PAUSED, EXCEPTION);
      put(CLOSED,    predecessors, NEW, CLOSING);

      PREDECESSORS = Collections.unmodifiableMap(predecessors);
    }

    /** Is the given transition valid? */
    static boolean isValid(State from, State to) {
      return PREDECESSORS.get(to).contains(from);
    }

    /** Validate the given transition. */
    static void validate(Object name, State from, State to) {
      LOG.debug("{}: {} -> {}", name, from, to);
      if (LOG.isTraceEnabled()) {
        LOG.trace("TRACE", new Throwable());
      }

      Preconditions.assertTrue(isValid(from, to),
          "ILLEGAL TRANSITION: In %s, %s -> %s", name, from, to);
    }
  }

  private volatile String name;
  private final AtomicReference<State> current = new AtomicReference<>(State.NEW);

  public LifeCycle(Object name) {
    this.name = name.toString();
    LOG.debug("{}: {}", name, current);
  }

  public void setName(String name) {
    this.name = name;
  }

  /** Transition from the current state to the given state. */
  public void transition(final State to) {
    final State from = current.getAndSet(to);
    State.validate(name, from, to);
  }

  /**
   * If the current state is equal to the specified from state,
   * then transition to the give to state; otherwise, make no change.
   *
   * @return true iff the current state is equal to the specified from state.
   */
  public boolean compareAndTransition(final State from, final State to) {
    if (current.compareAndSet(from, to)) {
      State.validate(name, from, to);
      return true;
    }
    return false;
  }

  /** @return the current state. */
  public State getCurrentState() {
    return current.get();
  }

  /** Assert if the current state equals to one of the expected states. */
  public void assertCurrentState(State... expected) {
    final State c = getCurrentState();
    if (!c.isOneOf(expected)) {
      throw new IllegalStateException("STATE MISMATCHED: In " + name
          + ", current state " + c + " is not one of the expected states "
          + Arrays.toString(expected));
    }
  }

  @Override
  public String toString() {
    return name + ":" + getCurrentState();
  }

  /** Run the given start method and transition the current state accordingly. */
  @SafeVarargs
  public final <T extends Throwable> void startAndTransition(
      CheckedRunnable<T> startImpl, Class<? extends Throwable>... exceptionClasses)
      throws T {
    transition(State.STARTING);
    try {
      startImpl.run();
      transition(State.RUNNING);
    } catch (Throwable t) {
      transition(ReflectionUtils.isInstance(t, exceptionClasses)?
          State.NEW: State.EXCEPTION);
      throw t;
    }
  }


  /**
   * Check the current state and, if applicable, run the given close method.
   * This method can be called multiple times
   * while the given close method will only be executed at most once.
   */
  public <T extends Throwable> void checkStateAndClose(
      CheckedRunnable<T> closeImpl) throws T {
    if (compareAndTransition(State.NEW, State.CLOSED)) {
      return;
    }

    for(;;) {
      final State c = getCurrentState();
      if (c.isOneOf(State.CLOSING, State.CLOSED)) {
        return; //already closing or closed.
      }

      if (compareAndTransition(c, State.CLOSING)) {
        try {
          closeImpl.run();
        } finally {
          transition(State.CLOSED);
        }
        return;
      }

      // lifecycle state is changed, retry.
    }
  }
}
