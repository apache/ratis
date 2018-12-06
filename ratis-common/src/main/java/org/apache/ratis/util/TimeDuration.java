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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.LongUnaryOperator;

/**
 * Time duration is represented together with a {@link TimeUnit}.
 *
 * This class is immutable.
 */
public class TimeDuration implements Comparable<TimeDuration> {

  public enum Abbreviation {
    NANOSECONDS("ns", "nanos"),
    MICROSECONDS("us", "μs", "micros"),
    MILLISECONDS("ms", "msec", "millis"),
    SECONDS("s", "sec"),
    MINUTES("m", "min"),
    HOURS("h", "hr"),
    DAYS("d");

    private final TimeUnit unit = TimeUnit.valueOf(name());
    private final List<String> symbols;

    Abbreviation(String... symbols) {
      final List<String> input = Arrays.asList(symbols);
      final List<String> all = new ArrayList<>(input.size() + 2);
      input.forEach(s -> all.add(s.toLowerCase()));

      final String s = unit.name().toLowerCase();
      all.add(s);
      all.add(s.substring(0, s.length() - 1));

      this.symbols = Collections.unmodifiableList(all);
    }

    public TimeUnit unit() {
      return unit;
    }

    String getDefault() {
      return symbols.get(0);
    }

    public List<String> getSymbols() {
      return symbols;
    }

    public static Abbreviation valueOf(TimeUnit unit) {
      return valueOf(unit.name());
    }
  }

  public static long parse(String timeString, TimeUnit targetUnit) {
    return valueOf(timeString, targetUnit).toLong(targetUnit);
  }

  /**
   * Parse the given time duration string.
   * If there is no unit specified, use the default unit.
   */
  public static TimeDuration valueOf(String timeString, TimeUnit defaultUnit) {
    final String lower = Objects.requireNonNull(timeString, "timeString = null").trim();
    for(Abbreviation a : Abbreviation.values()) {
      for(String s : a.getSymbols()) {
        if (lower.endsWith(s)) {
          final String value = lower.substring(0, lower.length()-s.length()).trim();
          try {
            return valueOf(Long.parseLong(value), a.unit());
          } catch(NumberFormatException e) {
            // failed with current symbol; ignore and try next symbol.
          }
        }
      }
    }
    return valueOf(Long.parseLong(lower), defaultUnit);
  }

  public static TimeDuration valueOf(long duration, TimeUnit unit) {
    return new TimeDuration(duration, unit);
  }

  private final long duration;
  private final TimeUnit unit;

  private TimeDuration(long duration, TimeUnit unit) {
    this.duration = duration;
    this.unit = Objects.requireNonNull(unit, "unit = null");
  }

  public long getDuration() {
    return duration;
  }

  public TimeUnit getUnit() {
    return unit;
  }

  /**
   * Convert this {@link TimeDuration} to a long in the target unit.
   * Note that the returned value may be truncated or saturated; see {@link TimeUnit#convert(long, TimeUnit)}.*
   *
   * @return the value in the target unit.
   */
  public long toLong(TimeUnit targetUnit) {
    return targetUnit.convert(duration, unit);
  }

  /**
   * The same as Math.toIntExact(toLong(targetUnit));
   * Similar to {@link #toLong(TimeUnit)}, the returned value may be truncated.
   * However, the returned value is never saturated.  The method throws {@link ArithmeticException} if it overflows.
   *
   * @return the value in the target unit.
   * @throws ArithmeticException if it overflows.
   */
  public int toInt(TimeUnit targetUnit) {
    return Math.toIntExact(toLong(targetUnit));
  }

  /** @return the {@link TimeDuration} in the target unit. */
  public TimeDuration to(TimeUnit targetUnit) {
    return this.unit == targetUnit? this: valueOf(toLong(targetUnit), targetUnit);
  }

  /** @return (this - that) in the minimum unit among them. */
  public TimeDuration minus(TimeDuration that) {
    Objects.requireNonNull(that, "that == null");
    final TimeUnit minUnit = CollectionUtils.min(this.unit, that.unit);
    return valueOf(this.toLong(minUnit) - that.toLong(minUnit), minUnit);
  }

  /** Round up to the given nanos to nearest multiple (in nanoseconds) of this {@link TimeDuration}. */
  public long roundUp(long nanos) {
    if (duration <= 0) {
      throw new ArithmeticException(
          "Rounding up to a non-positive " + getClass().getSimpleName() + " (=" + this + ")");
    }

    final long divisor = unit.toNanos(duration);
    if (nanos == 0 || divisor == 1) {
      return nanos;
    }

    long remainder = nanos % divisor; // In Java, the sign of remainder is the same as the dividend.
    if (remainder > 0) {
      remainder -= divisor;
    }
    return nanos - remainder;
  }

  /**
   * Apply the given operator to the duration value of this object.
   *
   * @return a new object with the new duration value and the same unit of this object.
   */
  public TimeDuration apply(LongUnaryOperator operator) {
    return valueOf(operator.applyAsLong(duration), unit);
  }

  public boolean isNegative() {
    return duration < 0;
  }

  public void sleep() throws InterruptedException {
    unit.sleep(duration);
  }

  @Override
  public int compareTo(TimeDuration that) {
    if (this.unit.compareTo(that.unit) > 0) {
      return that.compareTo(this);
    }
    // this.unit <= that.unit
    final long thisDurationInThatUnit = that.unit.convert(this.duration, this.unit);
    if (thisDurationInThatUnit == that.duration) {
      final long thatDurationInThisUnit = this.unit.convert(that.duration, that.unit);
      return Long.compare(this.duration, thatDurationInThisUnit);
    } else {
      return Long.compare(thisDurationInThatUnit, that.duration);
    }
  }

  @Override
  public String toString() {
    return duration + " " + Abbreviation.valueOf(unit).getDefault();
  }
}
