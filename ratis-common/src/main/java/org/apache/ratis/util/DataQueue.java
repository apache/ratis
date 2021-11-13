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

import org.apache.ratis.util.function.CheckedFunctionWithTimeout;
import org.apache.ratis.util.function.TriConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.ToLongFunction;

/**
 * A queue for data elements
 * such that the queue imposes limits on both number of elements and the data size in bytes.
 *
 * Null element is NOT supported.
 *
 * This class is NOT threadsafe.
 */
public class DataQueue<E> implements Iterable<E> {
  public static final Logger LOG = LoggerFactory.getLogger(DataQueue.class);

  private final Object name;
  private final long byteLimit;
  private final int elementLimit;
  private final ToLongFunction<E> getNumBytes;

  private final Queue<E> q;

  private long numBytes = 0;

  public DataQueue(Object name, SizeInBytes byteLimit, int elementLimit,
      ToLongFunction<E> getNumBytes) {
    this.name = name != null? name: this;
    this.byteLimit = byteLimit.getSize();
    this.elementLimit = elementLimit;
    this.getNumBytes = getNumBytes;
    this.q = new LinkedList<>();
  }

  public int getElementLimit() {
    return elementLimit;
  }

  public long getByteLimit() {
    return byteLimit;
  }

  public long getNumBytes() {
    return numBytes;
  }

  public int getNumElements() {
    return q.size();
  }

  /** The same as {@link java.util.Collection#isEmpty()}. */
  public final boolean isEmpty() {
    return getNumElements() == 0;
  }

  /** The same as {@link java.util.Collection#clear()}. */
  public void clear() {
    q.clear();
    numBytes = 0;
  }

  /**
   * Adds an element to this queue.
   *
   * @return true if the element is added successfully;
   *         otherwise, the element is not added, return false.
   */
  public boolean offer(E element) {
    Objects.requireNonNull(element, "element == null");
    if (elementLimit > 0 && q.size() >= elementLimit) {
      return false;
    }
    final long elementNumBytes = getNumBytes.applyAsLong(element);
    Preconditions.assertTrue(elementNumBytes >= 0,
        () -> name + ": elementNumBytes = " + elementNumBytes + " < 0");
    if (byteLimit > 0) {
      Preconditions.assertTrue(elementNumBytes <= byteLimit,
          () -> name + ": elementNumBytes = " + elementNumBytes + " > byteLimit = " + byteLimit);
      if (numBytes > byteLimit - elementNumBytes) {
        return false;
      }
    }
    q.offer(element);
    numBytes += elementNumBytes;
    return true;
  }

  /** Poll a list of the results within the given timeout. */
  public <RESULT, THROWABLE extends Throwable> List<RESULT> pollList(long timeoutMs,
      CheckedFunctionWithTimeout<E, RESULT, THROWABLE> getResult,
      TriConsumer<E, TimeDuration, TimeoutException> timeoutHandler) throws THROWABLE {
    if (timeoutMs <= 0 || q.isEmpty()) {
      return Collections.emptyList();
    }

    final Timestamp startTime = Timestamp.currentTime();
    final TimeDuration limit = TimeDuration.valueOf(timeoutMs, TimeUnit.MILLISECONDS);
    for(final List<RESULT> results = new ArrayList<>();;) {
      final E peeked = q.peek();
      if (peeked == null) { // q is empty
        return results;
      }

      final TimeDuration remaining = limit.subtract(startTime.elapsedTime());
      try {
        results.add(getResult.apply(peeked, remaining));
      } catch (TimeoutException e) {
        Optional.ofNullable(timeoutHandler).ifPresent(h -> h.accept(peeked, remaining, e));
        return results;
      }

      final E polled = poll();
      Preconditions.assertTrue(polled == peeked);
    }
  }

  /** Poll out the head element from this queue. */
  public E poll() {
    final E polled = q.poll();
    if (polled != null) {
      numBytes -= getNumBytes.applyAsLong(polled);
    }
    return polled;
  }

  /** The same as {@link java.util.Collection#remove(Object)}. */
  public boolean remove(E e) {
    final boolean removed = q.remove(e);
    if (removed) {
      numBytes -= getNumBytes.applyAsLong(e);
    }
    return removed;
  }

  @Override
  public Iterator<E> iterator() {
    final Iterator<E> i = q.iterator();
    // Do not support the remove() method.
    return new Iterator<E>() {
      @Override
      public boolean hasNext() {
        return i.hasNext();
      }

      @Override
      public E next() {
        return i.next();
      }
    };
  }
}
