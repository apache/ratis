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
package org.apache.ratis.util;

import org.apache.ratis.util.function.CheckedRunnable;
import org.apache.ratis.util.function.CheckedSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.UnaryOperator;

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

    /** Is this {@link State#RUNNING}? */
    public boolean isRunning() {
      return this == RUNNING;
    }

    /** Is this {@link State#CLOSING} or {@link State#CLOSED}? */
    public boolean isClosingOrClosed() {
      return States.CLOSING_OR_CLOSED.contains(this);
    }

    /** Is this {@link State#PAUSING} or {@link State#PAUSED}? */
    public boolean isPausingOrPaused() {
      return States.PAUSING_OR_PAUSED.contains(this);
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
    public static boolean isValid(State from, State to) {
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

  public static final class States {

    public static final Set<State> RUNNING
        = Collections.unmodifiableSet(EnumSet.of(State.RUNNING));

    public static final Set<State> STARTING_OR_RUNNING
        = Collections.unmodifiableSet(EnumSet.of(State.STARTING, State.RUNNING));

    public static final Set<State> CLOSING_OR_CLOSED
        = Collections.unmodifiableSet(EnumSet.of(State.CLOSING, State.CLOSED));

    public static final Set<State> PAUSING_OR_PAUSED
        = Collections.unmodifiableSet(EnumSet.of(State.PAUSING, State.PAUSED));

    public static final Set<State> CLOSING_OR_CLOSED_OR_EXCEPTION
        = Collections.unmodifiableSet(EnumSet.of(State.CLOSING, State.CLOSED, State.EXCEPTION));

    private States() {
      // no instances
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

  /** Transition from the current state to the given state if the current state is not equal to the given state. */
  public void transitionIfNotEqual(final State to) {
    final State from = current.getAndSet(to);
    if (from != to) {
      State.validate(name, from, to);
    }
  }

  /**
   * Transition from the current state to the given state only if the transition is valid.
   * If the transition is invalid, this is a no-op.
   *
   * @return true if the updated state equals to the given state.
   */
  public boolean transitionIfValid(final State to) {
    final State updated = current.updateAndGet(from -> State.isValid(from, to)? to : from);
    return updated == to;
  }

  /**
   * Transition using the given operator.
   *
   * @return the updated state if there is a transition;
   *         otherwise, return null to indicate no state change.
   */
  public State transition(UnaryOperator<State> operator) {
    for(;;) {
      final State previous = current.get();
      final State applied = operator.apply(previous);
      if (previous == applied) {
        return null; // no change required
      }
      State.validate(name, previous, applied);
      if (current.compareAndSet(previous, applied)) {
        return applied;
      }
      // state has been changed, retry
    }
  }

  /**
   * Transition using the given operator.
   *
   * @return the updated state.
   */
  public State transitionAndGet(UnaryOperator<State> operator) {
    return current.updateAndGet(previous -> {
      final State applied = operator.apply(previous);
      if (applied != previous) {
        State.validate(name, previous, applied);
      }
      return applied;
    });
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
  public void assertCurrentState(Set<State> expected) {
    assertCurrentState((n, c) -> new IllegalStateException("STATE MISMATCHED: In "
        + n + ", current state " + c + " is not one of the expected states "
        + expected), expected);
  }

  /** Assert if the current state equals to one of the expected states. */
  public <T extends Throwable> State assertCurrentState(
      BiFunction<String, State, T> newThrowable, Set<State> expected) throws T {
    final State c = getCurrentState();
    if (!expected.contains(c)) {
      throw newThrowable.apply(name, c);
    }
    return c;
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
   * Check the current state and, if applicable, transit to {@link State#CLOSING}.
   * If this is already in {@link State#CLOSING} or {@link State#CLOSED},
   * then invoking this method has no effect.
   * In other words, this method can be safely called multiple times.
   */
  public State checkStateAndClose() {
    return checkStateAndClose(() -> State.CLOSING);
  }

  /**
   * Check the current state and, if applicable, run the given close method.
   * If this is already in {@link State#CLOSING} or {@link State#CLOSED},
   * then invoking this method has no effect.
   * In other words, this method can be safely called multiple times
   * while the given close method will only be executed at most once.
   */
  public <T extends Throwable> State checkStateAndClose(CheckedRunnable<T> closeMethod) throws T {
    return checkStateAndClose(() -> {
      try {
        closeMethod.run();
      } finally {
        transition(State.CLOSED);
      }
      return State.CLOSED;
    });
  }

  private <T extends Throwable> State checkStateAndClose(CheckedSupplier<State, T> closeMethod) throws T {
    if (compareAndTransition(State.NEW, State.CLOSED)) {
      return State.CLOSED;
    }

    for(;;) {
      final State c = getCurrentState();
      if (c.isClosingOrClosed()) {
        return c; //already closing or closed.
      }

      if (compareAndTransition(c, State.CLOSING)) {
        return closeMethod.get();
      }

      // lifecycle state is changed, retry.
    }
  }
}
