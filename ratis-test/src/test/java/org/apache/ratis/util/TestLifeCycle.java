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

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import static org.apache.ratis.util.LifeCycle.State.CLOSED;
import static org.apache.ratis.util.LifeCycle.State.CLOSING;
import static org.apache.ratis.util.LifeCycle.State.EXCEPTION;
import static org.apache.ratis.util.LifeCycle.State.NEW;
import static org.apache.ratis.util.LifeCycle.State.PAUSED;
import static org.apache.ratis.util.LifeCycle.State.PAUSING;
import static org.apache.ratis.util.LifeCycle.State.RUNNING;
import static org.apache.ratis.util.LifeCycle.State.STARTING;
import static org.apache.ratis.util.LifeCycle.State.isValid;
import static org.apache.ratis.util.LifeCycle.State.put;

public class TestLifeCycle {
  /**
   * Test if the successor map and the predecessor map are consistent.
   * {@link LifeCycle} uses predecessors to validate transitions
   * while this test uses successors.
   */
  @Test(timeout = 1000)
  public void testIsValid() throws Exception {
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
    states.stream().forEach(
        from -> states.forEach(
            to -> Assert.assertEquals(from + " -> " + to,
                successors.get(from).contains(to),
                isValid(from, to))));
  }
}
