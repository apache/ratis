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

import org.apache.ratis.util.function.CheckedBiFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.LongUnaryOperator;

/**
 * Time duration is represented by a long together with a {@link TimeUnit}.
 *
 * This is a value-based class.
 */
public final class TimeDuration implements Comparable<TimeDuration> {
  static final Logger LOG = LoggerFactory.getLogger(TimeDuration.class);

  public static final TimeDuration ZERO = valueOf(0, TimeUnit.NANOSECONDS);
  public static final TimeDuration ONE_MILLISECOND = TimeDuration.valueOf(1, TimeUnit.MILLISECONDS);
  public static final TimeDuration ONE_SECOND = TimeDuration.valueOf(1, TimeUnit.SECONDS);
  public static final TimeDuration ONE_MINUTE = TimeDuration.valueOf(1, TimeUnit.MINUTES);

  static final double ERROR_THRESHOLD = 0.001; // accept 0.1% error

  /** @return the next lower {@link TimeUnit}.  If the unit is already the lowest, return it. */
  public static TimeUnit lowerUnit(TimeUnit unit) {
    final int ordinal = unit.ordinal();
    return ordinal == 0? unit: TimeUnit.values()[ordinal - 1];
  }

  /** @return the next higher {@link TimeUnit}.  If the unit is already the highest, return it. */
  public static TimeUnit higherUnit(TimeUnit unit) {
    final int ordinal = unit.ordinal();
    final TimeUnit[] timeUnits = TimeUnit.values();
    return ordinal == timeUnits.length - 1? unit: timeUnits[ordinal + 1];
  }

  /** Abbreviations of {@link TimeUnit}. */
  public enum Abbreviation {
    NANOSECONDS("ns", "nanos"),
    MICROSECONDS("μs", "us", "micros"),
    MILLISECONDS("ms", "msec", "millis"),
    SECONDS("s", "sec"),
    MINUTES("min", "m"),
    HOURS("hr", "h"),
    DAYS("day", "d");

    private final TimeUnit unit = TimeUnit.valueOf(name());
    private final List<String> symbols;

    Abbreviation(String... symbols) {
      final List<String> input = Arrays.asList(symbols);
      final List<String> all = new ArrayList<>(input.size() + 2);
      input.forEach(s -> all.add(s.toLowerCase()));

      final String s = unit.name().toLowerCase();
      addIfAbsent(all, s);
      addIfAbsent(all, s.substring(0, s.length() - 1));

      this.symbols = Collections.unmodifiableList(all);
    }

    static void addIfAbsent(List<String> list, String toAdd) {
      for(String s : list) {
        if (toAdd.equals(s)) {
          return;
        }
      }
      list.add(toAdd);
    }

    /** @return the corresponding {@link TimeUnit}. */
    public TimeUnit unit() {
      return unit;
    }

    /** @return the default abbreviation. */
    String getDefault() {
      return symbols.get(0);
    }

    /** @return the entire abbreviation list for this unit. */
    public List<String> getSymbols() {
      return symbols;
    }

    /** @return the corresponding {@link Abbreviation}. */
    public static Abbreviation valueOf(TimeUnit unit) {
      return valueOf(unit.name());
    }
  }

  /** The same as valueOf(timeString, targetUnit).toLong(targetUnit). */
  public static long parse(String timeString, TimeUnit targetUnit) {
    return valueOf(timeString, targetUnit).toLong(targetUnit);
  }

  /**
   * Parse the given time duration string.
   * If no unit is specified, use the default unit.
   * This method support underscores in Numeric Literals as in Java 8.
   *
   * @return a {@link TimeDuration} in the target unit.
   */
  public static TimeDuration valueOf(String timeString, TimeUnit defaultUnit) {
    Objects.requireNonNull(timeString, "timeString = null");
    final String lower = timeString.trim().replace("_", "").toLowerCase();
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

  /** @return a {@link TimeDuration} representing the given duration and unit. */
  public static TimeDuration valueOf(long duration, TimeUnit unit) {
    return new TimeDuration(duration, unit);
  }

  private final long duration;
  private final TimeUnit unit;

  private TimeDuration(long duration, TimeUnit unit) {
    this.duration = duration;
    this.unit = Objects.requireNonNull(unit, "unit = null");
  }

  /** @return the duration value. */
  public long getDuration() {
    return duration;
  }

  /** @return the {@link TimeUnit}. */
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
  public int toIntExact(TimeUnit targetUnit) {
    return Math.toIntExact(toLong(targetUnit));
  }

  /** @return the {@link TimeDuration} in the target unit. */
  public TimeDuration to(TimeUnit targetUnit) {
    if (this.unit == targetUnit) {
      return this;
    }
    final TimeDuration t = valueOf(toLong(targetUnit), targetUnit);
    LOG.debug("{}.to({}) = {}", this, targetUnit, t);
    return t;
  }

  /** @return (this + that) in the minimum unit among them. */
  public TimeDuration add(TimeDuration that) {
    Objects.requireNonNull(that, "that == null");
    final TimeUnit minUnit = CollectionUtils.min(this.unit, that.unit);
    return valueOf(this.toLong(minUnit) + that.toLong(minUnit), minUnit);
  }

  /** @return (this + (thatDuration, thatUnit)) in the minimum unit among them. */
  public TimeDuration add(long thatDuration, TimeUnit thatUnit) {
    return add(TimeDuration.valueOf(thatDuration, thatUnit));
  }

  /** @return (this - that) in the minimum unit among them. */
  public TimeDuration subtract(TimeDuration that) {
    Objects.requireNonNull(that, "that == null");
    final TimeUnit minUnit = CollectionUtils.min(this.unit, that.unit);
    return valueOf(this.toLong(minUnit) - that.toLong(minUnit), minUnit);
  }

  private static boolean isMagnitudeLarge(long n) {
    return n > 1_000_000_000_000L || n < -1_000_000_000_000L;
  }

  /** @return this * multiplier; the result unit may be changed in order to reduce rounding/overflow error. */
  public TimeDuration multiply(double multiplier) {
    final double product = duration * multiplier;
    final long rounded = Math.round(product);

    if (unit.ordinal() != TimeUnit.values().length - 1) {
      // check overflow error
      if (rounded == Long.MAX_VALUE || rounded == Long.MIN_VALUE) {
        if (Math.abs(multiplier) > 2) {
          return multiply(2).multiply(multiplier / 2);
        } else {
          return to(higherUnit(unit)).multiply(multiplier);
        }
      }
    }
    if (unit.ordinal() != 0) {
      // check round off error
      if (Math.abs(product - rounded) > Math.abs(product) * ERROR_THRESHOLD) {
        if (isMagnitudeLarge(duration) && Math.abs(multiplier) < 0.5d) {
          return multiply(0.5).multiply(multiplier * 2);
        } else {
          return to(lowerUnit(unit)).multiply(multiplier);
        }
      }
    }

    final TimeDuration t = valueOf(rounded, unit);
    LOG.debug("{} * {} = {}", this, multiplier, t);
    return t;
  }

  /** @return -this. */
  public TimeDuration negate() {
    if (duration == Long.MIN_VALUE) {
      return valueOf(Long.MAX_VALUE, unit);
    }
    return valueOf(Math.negateExact(duration), unit);
  }

  /** Round up to the given nanos to nearest multiple (in nanoseconds) of this {@link TimeDuration}. */
  public long roundUpNanos(long nanos) {
    if (duration <= 0) {
      throw new ArithmeticException(
          "Rounding up to a non-positive " + JavaUtils.getClassSimpleName(getClass()) + " (=" + this + ")");
    }

    final long divisor = toLong(TimeUnit.NANOSECONDS);
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

  /** Apply the given function to the (duration, unit) of this object. */
  public <OUTPUT, THROWABLE extends Throwable> OUTPUT apply(
      CheckedBiFunction<Long, TimeUnit, OUTPUT, THROWABLE> function) throws THROWABLE {
    return function.apply(getDuration(), getUnit());
  }

  /** @return Is this {@link TimeDuration} negative? */
  public boolean isNegative() {
    return duration < 0;
  }

  /** @return Is this {@link TimeDuration} less than or equal to zero? */
  public boolean isNonPositive() {
    return duration <= 0;
  }

  /** The same as sleep(null). */
  public TimeDuration sleep() throws InterruptedException {
    return sleep(null);
  }

  /**
   * Performs a {@link TimeUnit#sleep(long)} using this {@link TimeDuration}.
   *
   * @param log If not null, use it to print log messages.
   * @return the difference of the actual sleep time duration and this {@link TimeDuration}.
   */
  public TimeDuration sleep(Consumer<Object> log) throws InterruptedException {
    if (log != null) {
      log.accept(StringUtils.stringSupplierAsObject(() -> "Start sleeping " + this));
    }
    final Timestamp start = Timestamp.currentTime();
    try {
      unit.sleep(duration);
      if (log != null) {
        log.accept(StringUtils.stringSupplierAsObject(() -> "Completed sleeping " + this));
      }
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      if (log != null) {
        log.accept(StringUtils.stringSupplierAsObject(() -> "Interrupted sleeping " + this));
      }
      throw ie;
    }
    return start.elapsedTime().subtract(this);
  }

  @Override
  public int compareTo(TimeDuration that) {
    if (this.unit.compareTo(that.unit) > 0) {
      return Integer.compare(0, that.compareTo(this));
    }
    if (this.unit == that.unit) {
      return Long.compare(this.duration, that.duration);
    }
    // this.unit < that.unit
    final long thisDurationInThatUnit = this.toLong(that.unit);
    if (thisDurationInThatUnit == that.duration) {
      // check for overflow
      final long thatDurationInThisUnit = that.toLong(this.unit);
      return Long.compare(this.duration, thatDurationInThisUnit);
    } else {
      return Long.compare(thisDurationInThatUnit, that.duration);
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (!(obj instanceof TimeDuration)) {
      return false;
    }
    final TimeDuration that = (TimeDuration)obj;
    return this.compareTo(that) == 0;
  }

  @Override
  public int hashCode() {
    return Long.hashCode(toLong(TimeUnit.NANOSECONDS));
  }

  @Override
  public String toString() {
    return duration + Abbreviation.valueOf(unit).getDefault();
  }
}
