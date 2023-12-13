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

import org.apache.ratis.util.function.TriConsumer;
import org.junit.Test;

import static org.apache.ratis.util.LifeCycle.State.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.*;

public class TestLifeCycle {
  /**
   * Test if the successor map and the predecessor map are consistent.
   * {@link LifeCycle} uses predecessors to validate transitions
   * while this test uses successors.
   */
  @Test(timeout = 1000)
  public void testIsValid() {
    final Map<LifeCycle.State, List<LifeCycle.State>> successors
        = new EnumMap<>(LifeCycle.State.class);
    put(NEW,       successors, STARTING, CLOSED);
    put(STARTING,  successors, NEW, RUNNING, CLOSING, EXCEPTION);
    put(RUNNING,   successors, CLOSING, PAUSING, EXCEPTION);
    put(PAUSING,   successors, PAUSED, CLOSING, EXCEPTION);
    put(PAUSED,    successors, STARTING, CLOSING);
    put(EXCEPTION, successors, CLOSING);
    put(CLOSING ,  successors, CLOSED);
    put(CLOSED,    successors);

    final List<LifeCycle.State> states = Arrays.asList(LifeCycle.State.values());
    states.forEach(
        from -> states.forEach(
            to -> assertEquals(from + " -> " + to,
                successors.get(from).contains(to),
                isValid(from, to))));
  }

  @Test
  public void validTransitions() {
    testValidTransition((from, subject, to) -> assertTrue(subject.compareAndTransition(from, to)));
    testValidTransition((from, subject, to) -> subject.transition(to));
    testValidTransition((from, subject, to) -> assertEquals(to, subject.transitionAndGet(any -> to)));
    testValidTransition((from, subject, to) -> subject.transitionIfNotEqual(to));
    testValidTransition((from, subject, to) -> assertTrue(subject.transitionIfValid(to)));
  }

  private static void testValidTransition(TriConsumer<LifeCycle.State, LifeCycle, LifeCycle.State> op) {
    LifeCycle subject = new LifeCycle("subject");
    for (LifeCycle.State to : new LifeCycle.State[] { STARTING, RUNNING, PAUSING, PAUSED, CLOSING, CLOSED }) {
      LifeCycle.State from = subject.getCurrentState();
      op.accept(from, subject, to);
      assertEquals(to, subject.getCurrentState());
    }
  }

  @Test
  public void invalidTransitions() {
    testInvalidTransition((from, subject, to) -> subject.compareAndTransition(from, to), true);
    testInvalidTransition((from, subject, to) -> subject.transition(to), true);
    testInvalidTransition((from, subject, to) -> subject.transitionIfNotEqual(to), true);
    testInvalidTransition((from, subject, to) -> assertFalse(subject.transitionIfValid(to)), false);
    testInvalidTransition((from, subject, to) -> subject.transitionAndGet(any -> to), true);
  }

  private static void testInvalidTransition(TriConsumer<LifeCycle.State, LifeCycle, LifeCycle.State> op, boolean shouldThrow) {
    LifeCycle subject = new LifeCycle("subject");
    for (LifeCycle.State to : new LifeCycle.State[] { RUNNING, EXCEPTION, CLOSING }) {
      LifeCycle.State from = subject.getCurrentState();
      try {
        op.accept(from, subject, to);
        assertFalse(shouldThrow);
      } catch (IllegalStateException e) {
        assertTrue(shouldThrow);
        assertEquals("Should be in original state", from, subject.getCurrentState());
      }
    }
  }

}
