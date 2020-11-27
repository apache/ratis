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

import java.util.concurrent.TimeUnit;

/**
 * Use {@link System#nanoTime()} as timestamps.
 *
 * This class takes care the possibility of numerical overflow.
 *
 * This is a value-based class.
 */
public final class Timestamp implements Comparable<Timestamp> {
  private static final long NANOSECONDS_PER_MILLISECOND = 1000000;

  private static final long START_TIME = System.nanoTime();

  /** @return a {@link Timestamp} for the given nanos. */
  public static Timestamp valueOf(long nanos) {
    return new Timestamp(nanos);
  }

  /** @return a long in nanos for the current time. */
  public static long currentTimeNanos() {
    return System.nanoTime();
  }

  /** @return a {@link Timestamp} for the current time. */
  public static Timestamp currentTime() {
    return valueOf(currentTimeNanos());
  }

  /** @return the latest timestamp. */
  public static Timestamp latest(Timestamp a, Timestamp b) {
    return a.compareTo(b) > 0? a: b;
  }

  private final long nanos;

  private Timestamp(long nanos) {
    this.nanos = nanos;
  }

  /**
   * @param milliseconds the time period to be added.
   * @return a new {@link Timestamp} whose value is calculated
   *         by adding the given milliseconds to this timestamp.
   */
  public Timestamp addTimeMs(long milliseconds) {
    return new Timestamp(nanos + milliseconds * NANOSECONDS_PER_MILLISECOND);
  }

  /**
   * @param t the time period to be added.
   * @return a new {@link Timestamp} whose value is calculated
   *         by adding the given milliseconds to this timestamp.
   */
  public Timestamp addTime(TimeDuration t) {
    return new Timestamp(nanos + t.to(TimeUnit.NANOSECONDS).getDuration());
  }

  /**
   * @return the elapsed time in milliseconds.
   *         If the timestamp is a future time, the returned value is negative.
   */
  public long elapsedTimeMs() {
    final long d = System.nanoTime() - nanos;
    return d / NANOSECONDS_PER_MILLISECOND;
  }

  /**
   * @return the elapsed time in nanoseconds.
   *         If the timestamp is a future time, the returned value is negative.
   */
  public TimeDuration elapsedTime() {
    final long d = System.nanoTime() - nanos;
    return TimeDuration.valueOf(d, TimeUnit.NANOSECONDS);
  }

  /**
   * Compare two timestamps, t0 (this) and t1 (that).
   * This method uses {@code t0 - t1 < 0}, not {@code t0 < t1},
   * in order to take care the possibility of numerical overflow.
   *
   * @see System#nanoTime()
   */
  @Override
  public int compareTo(Timestamp that) {
    final long d = this.nanos - that.nanos;
    return d > 0? 1: d == 0? 0: -1;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (!(obj instanceof Timestamp)) {
      return false;
    }
    final Timestamp that = (Timestamp)obj;
    return this.nanos == that.nanos;
  }

  @Override
  public int hashCode() {
    return Long.hashCode(nanos);
  }

  @Override
  public String toString() {
    final long ms = (nanos - START_TIME)/NANOSECONDS_PER_MILLISECOND;
    return (ms/1000) + "." + (ms%1000) + TimeDuration.Abbreviation.SECONDS.getDefault();
  }
}
