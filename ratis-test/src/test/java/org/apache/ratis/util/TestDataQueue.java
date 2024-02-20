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

import org.apache.ratis.util.function.TriConsumer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class TestDataQueue {
  static <T> TriConsumer<T, TimeDuration, TimeoutException> getTimeoutHandler(boolean expctedTimeout) {
    return (element, time, exception) -> {
      if (!expctedTimeout) {
        throw new AssertionError("Unexpected timeout to get element " + element + " in " + time, exception);
      }
    };
  }

  static void assertSizes(long expectedNumElements, long expectedNumBytes, DataQueue<?> q) {
    Assertions.assertEquals(expectedNumElements, q.getNumElements());
    Assertions.assertEquals(expectedNumBytes, q.getNumBytes());
  }

  final SizeInBytes byteLimit = SizeInBytes.valueOf(100);
  final int elementLimit = 5;
  final DataQueue<Long> q = new DataQueue<>(null, byteLimit, elementLimit, Long::longValue);

  @Test
  @Timeout(value = 1000)
  public void testElementLimit() {
    runTestElementLimit(q);
  }

  static void runTestElementLimit(DataQueue<Long> q) {
    assertSizes(0, 0, q);

    final int elementLimit = q.getElementLimit();
    long numBytes = 0;
    for (long i = 0; i < elementLimit; i++) {
      Assertions.assertEquals(i, q.getNumElements());
      Assertions.assertEquals(numBytes, q.getNumBytes());
      final boolean offered = q.offer(i);
      Assertions.assertTrue(offered);
      numBytes += i;
      assertSizes(i+1, numBytes, q);
    }
    {
      final boolean offered = q.offer(0L);
      Assertions.assertFalse(offered);
      assertSizes(elementLimit, numBytes, q);
    }

    { // poll all elements
      final List<Long> polled = q.pollList(100, (i, timeout) -> i, getTimeoutHandler(false));
      Assertions.assertEquals(elementLimit, polled.size());
      for (int i = 0; i < polled.size(); i++) {
        Assertions.assertEquals(i, polled.get(i).intValue());
      }
    }
    assertSizes(0, 0, q);
  }

  @Test
  @Timeout(value = 1000)
  public void testByteLimit() {
    runTestByteLimit(q);
  }

  static void runTestByteLimit(DataQueue<Long> q) {
    assertSizes(0, 0, q);

    final long byteLimit = q.getByteLimit();
    try {
      q.offer(byteLimit + 1);
      Assertions.fail();
    } catch (IllegalStateException ignored) {
    }

    final long halfBytes = byteLimit / 2;
    {
      final boolean offered = q.offer(halfBytes);
      Assertions.assertTrue(offered);
      assertSizes(1, halfBytes, q);
    }

    {
      final boolean offered = q.offer(halfBytes + 1);
      Assertions.assertFalse(offered);
      assertSizes(1, halfBytes, q);
    }

    {
      final boolean offered = q.offer(halfBytes);
      Assertions.assertTrue(offered);
      assertSizes(2, byteLimit, q);
    }

    {
      final boolean offered = q.offer(1L);
      Assertions.assertFalse(offered);
      assertSizes(2, byteLimit, q);
    }

    {
      final boolean offered = q.offer(0L);
      Assertions.assertTrue(offered);
      assertSizes(3, byteLimit, q);
    }

    { // poll all elements
      final List<Long> polled = q.pollList(100, (i, timeout) -> i, getTimeoutHandler(false));
      Assertions.assertEquals(3, polled.size());
      Assertions.assertEquals(halfBytes, polled.get(0).intValue());
      Assertions.assertEquals(halfBytes, polled.get(1).intValue());
      Assertions.assertEquals(0, polled.get(2).intValue());
    }

    assertSizes(0, 0, q);
  }

  @Test
  @Timeout(value = 1000)
  public void testIteratorAndRemove() {
    runTestIteratorAndRemove(q);
  }

  static void runTestIteratorAndRemove(DataQueue<Long> q) {
    assertSizes(0, 0, q);

    final int elementLimit = q.getElementLimit();
    int numElements = 0;
    long numBytes = 0;
    for(long i = 0; i < elementLimit; i++) {
      final boolean offered = q.offer(i);
      Assertions.assertTrue(offered);
      numElements++;
      numBytes += i;
      assertSizes(numElements, numBytes, q);
    }

    { // test iterator()
      final Iterator<Long> i = q.iterator();
      for (long expected = 0; expected < elementLimit; expected++) {
        Assertions.assertEquals(expected, i.next().longValue());
      }
    }

    { // test remove(..)
      final List<Long> toRemoves = new ArrayList<>(elementLimit);
      for (long i = 0; i < elementLimit; i++) {
        toRemoves.add(i);
      }
      Collections.shuffle(toRemoves);

      for (Long r : toRemoves) {
        q.remove(r);
        numElements--;
        numBytes -= r;
        assertSizes(numElements, numBytes, q);
      }
    }

    assertSizes(0, 0, q);
  }

  @Test
  @Timeout(value = 1000)
  public void testTimeout() {
    assertSizes(0, 0, q);

    long numBytes = 0;
    for (long i = 0; i < elementLimit; i++) {
      Assertions.assertEquals(i, q.getNumElements());
      Assertions.assertEquals(numBytes, q.getNumBytes());
      final boolean offered = q.offer(i);
      Assertions.assertTrue(offered);
      numBytes += i;
      assertSizes(i+1, numBytes, q);
    }

    { // poll with zero time
      final List<Long> polled = q.pollList(0, (i, timeout) -> i, getTimeoutHandler(false));
      Assertions.assertTrue(polled.isEmpty());
      assertSizes(elementLimit, numBytes, q);
    }

    final int halfElements = elementLimit / 2;
    { // poll with timeout
      final List<Long> polled = q.pollList(100, (i, timeout) -> {
        if (i == halfElements) {
          // simulate timeout
          throw new TimeoutException("i=" + i);
        }
        return i;
      }, getTimeoutHandler(true));
      Assertions.assertEquals(halfElements, polled.size());
      for (int i = 0; i < polled.size(); i++) {
        Assertions.assertEquals(i, polled.get(i).intValue());
        numBytes -= i;
      }
      assertSizes(elementLimit - halfElements, numBytes, q);
    }

    { // poll the remaining elements
      final List<Long> polled = q.pollList(100, (i, timeout) -> i, getTimeoutHandler(false));
      Assertions.assertEquals(elementLimit - halfElements, polled.size());
      for (int i = 0; i < polled.size(); i++) {
        Assertions.assertEquals(halfElements + i, polled.get(i).intValue());
      }
    }
    assertSizes(0, 0, q);
  }
}
