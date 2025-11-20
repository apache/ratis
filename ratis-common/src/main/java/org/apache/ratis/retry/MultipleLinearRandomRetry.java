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
package org.apache.ratis.retry;

import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.TimeDuration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Given pairs of number of retries and sleep time (n0, t0), (n1, t1), ...,
 * the first n0 retries sleep t0 milliseconds on average,
 * the following n1 retries sleep t1 milliseconds on average, and so on.
 * <p>
 * For all the sleep, the actual sleep time is randomly uniform distributed
 * in the close interval [0.5t, 1.5t], where t is the sleep time specified.
 * <p>
 * The objects of this class are immutable.
 */
public final class MultipleLinearRandomRetry implements RetryPolicy {
  /** Pairs of numRetries and sleepSeconds */
  private static final class Pair {
    private final int numRetries;
    private final TimeDuration sleepTime;

    Pair(int numRetries, TimeDuration sleepTime) {
      this.numRetries = numRetries;
      this.sleepTime = sleepTime;
    }

    TimeDuration getRandomSleepTime() {
      // 0.5 <= ratio < 1.5
      final double ratio = ThreadLocalRandom.current().nextDouble() + 0.5;
      return sleepTime.multiply(ratio);
    }

    @Override
    public String toString() {
      return numRetries + "x" + sleepTime;
    }
  }

  private final List<Pair> pairs;
  private final Supplier<String> myString;

  private MultipleLinearRandomRetry(List<Pair> pairs) {
    this.pairs = Collections.unmodifiableList(pairs);
    this.myString = JavaUtils.memoize(() -> JavaUtils.getClassSimpleName(getClass()) + pairs);
  }

  @Override
  public Action handleAttemptFailure(Event event) {
    final Pair p = searchPair(event.getAttemptCount());
    return p == null? NO_RETRY_ACTION: p::getRandomSleepTime;
  }

  /**
   * Given the current number of retry, search the corresponding pair.
   * @return the corresponding pair,
   *   or null if the current number of retry > maximum number of retry.
   */
  private Pair searchPair(int curRetry) {
    int i = 0;
    for(; i < pairs.size() && curRetry > pairs.get(i).numRetries; i++) {
      curRetry -= pairs.get(i).numRetries;
    }
    return i == pairs.size()? null: pairs.get(i);
  }

  @Override
  public int hashCode() {
    return toString().hashCode();
  }

  @Override
  public boolean equals(final Object that) {
    if (this == that) {
      return true;
    } else if (that == null || this.getClass() != that.getClass()) {
      return false;
    }
    return this.toString().equals(that.toString());
  }

  @Override
  public String toString() {
    return myString.get();
  }

  /**
   * Parse the given string as a MultipleLinearRandomRetry object.
   * The format of the string is "t_1, n_1, t_2, n_2, ...",
   * where t_i and n_i are the i-th pair of sleep time and number of retries.
   * Note that the white spaces in the string are ignored.
   *
   * @return the parsed object, or null if the parsing fails.
   */
  public static MultipleLinearRandomRetry parseCommaSeparated(String input) {
    input = input.trim();
    if (input.isEmpty()) {
      throw new IllegalArgumentException("Failed to parse \"" + input + "\": no elements found");
    }
    final String[] elements = input.split(",");
    if (elements.length % 2 != 0) {
      throw new IllegalArgumentException("Failed to parse \"" + input
          + "\": number of elements (" + elements.length + ") is old");
    }

    final List<Pair> pairs = new ArrayList<>();
    for(int i = 0; i < elements.length; ) {
      final TimeDuration sleep = parseElement("sleep-time", elements, i++, input,
          MultipleLinearRandomRetry::parsePositiveTime);
      final Integer retries = parseElement("retries", elements, i++, input,
          MultipleLinearRandomRetry::parsePositiveInt);
      pairs.add(new Pair(retries, sleep));
    }
    return new MultipleLinearRandomRetry(pairs);
  }

  private static TimeDuration parsePositiveTime(String s) {
    final TimeDuration t = TimeDuration.valueOf(s, TimeUnit.MILLISECONDS);
    if (t.isNonPositive()) {
      throw new IllegalArgumentException("Non-positive value: " + t);
    }
    return t;
  }

  private static int parsePositiveInt(String trimmed) {
    final int n = Integer.parseInt(trimmed);
    if (n <= 0) {
      throw new IllegalArgumentException("Non-positive value: " + n);
    }
    return n;
  }

  private static <E> E parseElement(String name, String[] elements, int i, String input, Function<String, E> parser) {
    final String s = elements[i].trim().replace("_", "");
    try {
      return parser.apply(s);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Failed to parse \"" + s + "\" as " + name + " (element " + i + " in \"" + input + "\")", e);
    }
  }
}
