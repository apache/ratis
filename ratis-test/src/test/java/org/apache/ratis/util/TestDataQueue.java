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
import org.junit.Assert;
import org.junit.Test;

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
    Assert.assertEquals(expectedNumElements, q.getNumElements());
    Assert.assertEquals(expectedNumBytes, q.getNumBytes());
  }

  final SizeInBytes byteLimit = SizeInBytes.valueOf(100);
  final int elementLimit = 5;
  final DataQueue<Long> q = new DataQueue<Long>(null, byteLimit, elementLimit, Long::longValue);

  @Test(timeout = 1000)
  public void testElementLimit() {
    runTestElementLimit(q);
  }

  static void runTestElementLimit(DataQueue<Long> q) {
    assertSizes(0, 0, q);

    final int elementLimit = q.getElementLimit();
    int numBytes = 0;
    for (long i = 0; i < elementLimit; i++) {
      Assert.assertEquals(i, q.getNumElements());
      Assert.assertEquals(numBytes, q.getNumBytes());
      final boolean offered = q.offer(i);
      Assert.assertTrue(offered);
      numBytes += i;
      assertSizes(i+1, numBytes, q);
    }
    {
      final boolean offered = q.offer(0L);
      Assert.assertFalse(offered);
      assertSizes(elementLimit, numBytes, q);
    }

    { // poll all elements
      final List<Long> polled = q.pollList(100, (i, timeout) -> i, getTimeoutHandler(false));
      Assert.assertEquals(elementLimit, polled.size());
      for (int i = 0; i < polled.size(); i++) {
        Assert.assertEquals(i, polled.get(i).intValue());
      }
    }
    assertSizes(0, 0, q);
  }

  @Test(timeout = 1000)
  public void testByteLimit() {
    runTestByteLimit(q);
  }

  static void runTestByteLimit(DataQueue<Long> q) {
    assertSizes(0, 0, q);

    final long byteLimit = q.getByteLimit();
    try {
      q.offer(byteLimit + 1);
      Assert.fail();
    } catch (IllegalStateException ignored) {
    }

    final long halfBytes = byteLimit / 2;
    {
      final boolean offered = q.offer(halfBytes);
      Assert.assertTrue(offered);
      assertSizes(1, halfBytes, q);
    }

    {
      final boolean offered = q.offer(halfBytes + 1);
      Assert.assertFalse(offered);
      assertSizes(1, halfBytes, q);
    }

    {
      final boolean offered = q.offer(halfBytes);
      Assert.assertTrue(offered);
      assertSizes(2, byteLimit, q);
    }

    {
      final boolean offered = q.offer(1L);
      Assert.assertFalse(offered);
      assertSizes(2, byteLimit, q);
    }

    {
      final boolean offered = q.offer(0L);
      Assert.assertTrue(offered);
      assertSizes(3, byteLimit, q);
    }

    { // poll all elements
      final List<Long> polled = q.pollList(100, (i, timeout) -> i, getTimeoutHandler(false));
      Assert.assertEquals(3, polled.size());
      Assert.assertEquals(halfBytes, polled.get(0).intValue());
      Assert.assertEquals(halfBytes, polled.get(1).intValue());
      Assert.assertEquals(0, polled.get(2).intValue());
    }

    assertSizes(0, 0, q);
  }

  @Test(timeout = 1000)
  public void testIteratorAndRemove() {
    runTestIteratorAndRemove(q);
  }

  static void runTestIteratorAndRemove(DataQueue<Long> q) {
    assertSizes(0, 0, q);

    final int elementLimit = q.getElementLimit();
    int numElements = 0;
    int numBytes = 0;
    for(long i = 0; i < elementLimit; i++) {
      final boolean offered = q.offer(i);
      Assert.assertTrue(offered);
      numElements++;
      numBytes += i;
      assertSizes(numElements, numBytes, q);
    }

    { // test iterator()
      final Iterator<Long> i = q.iterator();
      for (long expected = 0; expected < elementLimit; expected++) {
        Assert.assertEquals(expected, i.next().longValue());
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

  @Test(timeout = 1000)
  public void testTimeout() {
    assertSizes(0, 0, q);

    int numBytes = 0;
    for (long i = 0; i < elementLimit; i++) {
      Assert.assertEquals(i, q.getNumElements());
      Assert.assertEquals(numBytes, q.getNumBytes());
      final boolean offered = q.offer(i);
      Assert.assertTrue(offered);
      numBytes += i;
      assertSizes(i+1, numBytes, q);
    }

    { // poll with zero time
      final List<Long> polled = q.pollList(0, (i, timeout) -> i, getTimeoutHandler(false));
      Assert.assertTrue(polled.isEmpty());
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
      Assert.assertEquals(halfElements, polled.size());
      for (int i = 0; i < polled.size(); i++) {
        Assert.assertEquals(i, polled.get(i).intValue());
        numBytes -= i;
      }
      assertSizes(elementLimit - halfElements, numBytes, q);
    }

    { // poll the remaining elements
      final List<Long> polled = q.pollList(100, (i, timeout) -> i, getTimeoutHandler(false));
      Assert.assertEquals(elementLimit - halfElements, polled.size());
      for (int i = 0; i < polled.size(); i++) {
        Assert.assertEquals(halfElements + i, polled.get(i).intValue());
      }
    }
    assertSizes(0, 0, q);
  }
}
