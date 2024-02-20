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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.event.Level;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.ratis.util.TimeDuration.Abbreviation;
import static org.apache.ratis.util.TimeDuration.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestTimeDuration {
  {
    Slf4jUtils.setLogLevel(TimeDuration.LOG, Level.DEBUG);
  }

  @Test
  @Timeout(value = 1000)
  public void testAbbreviation() {
    Arrays.asList(TimeUnit.values())
        .forEach(a -> assertNotNull(Abbreviation.valueOf(a.name())));
    assertEquals(TimeUnit.values().length, Abbreviation.values().length);

    final List<String> allSymbols = Arrays.stream(Abbreviation.values())
        .map(Abbreviation::getSymbols)
        .flatMap(List::stream)
        .collect(Collectors.toList());
    Arrays.asList(TimeUnit.values()).forEach(unit ->
        allSymbols.stream()
            .map(s -> "0" + s)
            .forEach(s -> assertEquals(0L, parse(s, unit), s)));
  }

  @Test
  @Timeout(value = 1000)
  public void testParse() {
    assertEquals(1L, parse("1_000_000 ns", TimeUnit.MILLISECONDS));
    assertEquals(10L, parse("10_000_000 nanos", TimeUnit.MILLISECONDS));
    assertEquals(100L, parse("100_000_000 nanosecond", TimeUnit.MILLISECONDS));
    assertEquals(1000L, parse("1_000_000_000 nanoseconds", TimeUnit.MILLISECONDS));

    assertEquals(1L, parse("1000 us", TimeUnit.MILLISECONDS));
    assertEquals(10L, parse("10000 μs", TimeUnit.MILLISECONDS));
    assertEquals(100L, parse("100000 micros", TimeUnit.MILLISECONDS));
    assertEquals(1000L, parse("1000000 microsecond", TimeUnit.MILLISECONDS));
    assertEquals(10000L, parse("10000000 microseconds", TimeUnit.MILLISECONDS));

    assertEquals(1L, parse("1 ms", TimeUnit.MILLISECONDS));
    assertEquals(10L, parse("10 msec", TimeUnit.MILLISECONDS));
    assertEquals(100L, parse("100 millis", TimeUnit.MILLISECONDS));
    assertEquals(1000L, parse("1000 millisecond", TimeUnit.MILLISECONDS));
    assertEquals(10000L, parse("10000 milliseconds", TimeUnit.MILLISECONDS));

    assertEquals(1000L, parse("1 s", TimeUnit.MILLISECONDS));
    assertEquals(10000L, parse("10 sec", TimeUnit.MILLISECONDS));
    assertEquals(100000L, parse("100 second", TimeUnit.MILLISECONDS));
    assertEquals(1000000L, parse("1000 seconds", TimeUnit.MILLISECONDS));

    assertEquals(60, parse("1 m", TimeUnit.SECONDS));
    assertEquals(600, parse("10 min", TimeUnit.SECONDS));
    assertEquals(6000, parse("100 minutes", TimeUnit.SECONDS));
    assertEquals(60000, parse("1000 minutes", TimeUnit.SECONDS));

    assertEquals(60, parse("1 h", TimeUnit.MINUTES));
    assertEquals(600, parse("10 hr", TimeUnit.MINUTES));
    assertEquals(6000, parse("100 hour", TimeUnit.MINUTES));
    assertEquals(60000, parse("1000 hours", TimeUnit.MINUTES));

    assertEquals(24, parse("1 d", TimeUnit.HOURS));
    assertEquals(240, parse("10 day", TimeUnit.HOURS));
    assertEquals(2400, parse("100 days", TimeUnit.HOURS));
  }

  @Test
  @Timeout(value = 1000)
  public void testRoundUp() {
    final long nanosPerSecond = 1_000_000_000L;
    final TimeDuration oneSecond = TimeDuration.valueOf(1, TimeUnit.SECONDS);
    assertEquals(-nanosPerSecond, oneSecond.roundUpNanos(-nanosPerSecond - 1));
    assertEquals(-nanosPerSecond, oneSecond.roundUpNanos(-nanosPerSecond));
    assertEquals(0, oneSecond.roundUpNanos(-nanosPerSecond + 1));
    assertEquals(0, oneSecond.roundUpNanos(-1));
    assertEquals(0, oneSecond.roundUpNanos(0));
    assertEquals(nanosPerSecond, oneSecond.roundUpNanos(1));
    assertEquals(nanosPerSecond, oneSecond.roundUpNanos(nanosPerSecond - 1));
    assertEquals(nanosPerSecond, oneSecond.roundUpNanos(nanosPerSecond));
    assertEquals(2*nanosPerSecond, oneSecond.roundUpNanos(nanosPerSecond + 1));
  }

  @Test
  @Timeout(value = 1000)
  public void testTo() {
    final TimeDuration oneSecond = TimeDuration.valueOf(1, TimeUnit.SECONDS);
    assertTo(1000, "1000ms", oneSecond, TimeUnit.MILLISECONDS);
    final TimeDuration nanos = assertTo(1_000_000_000, "1000000000ns", oneSecond, TimeUnit.NANOSECONDS);
    assertTo(1000, "1000ms", nanos, TimeUnit.MILLISECONDS);

    assertTo(0, "0.0167min", oneSecond, TimeUnit.MINUTES);
    assertTo(0, "0.0167min", nanos, TimeUnit.MINUTES);

    final TimeDuration millis = TimeDuration.valueOf(1_999, TimeUnit.MILLISECONDS);
    assertTo(1, "1.9990s", millis, TimeUnit.SECONDS);
    assertTo(0, "0.0333min", millis, TimeUnit.MINUTES);
  }

  static TimeDuration assertTo(long expected, String expectedString, TimeDuration timeDuration, TimeUnit toUnit) {
    final TimeDuration computed = timeDuration.to(toUnit);
    assertEquals(expected, computed.getDuration());
    assertEquals(toUnit, computed.getUnit());
    assertEquals(expectedString, timeDuration.toString(toUnit, 4));
    return computed;
  }

  @Test
  @Timeout(value = 1000)
  public void testAddAndSubtract() {
    final TimeDuration oneSecond = TimeDuration.valueOf(1, TimeUnit.SECONDS);
    final TimeDuration tenSecond = TimeDuration.valueOf(10, TimeUnit.SECONDS);
    {
      final TimeDuration d = oneSecond.subtract(oneSecond);
      assertEquals(0, d.getDuration());
      assertEquals(TimeUnit.SECONDS, d.getUnit());

      final TimeDuration sum = d.add(oneSecond);
      assertEquals(1, sum.getDuration());
      assertEquals(TimeUnit.SECONDS, sum.getUnit());
    }
    {
      final TimeDuration d = tenSecond.subtract(oneSecond);
      assertEquals(9, d.getDuration());
      assertEquals(TimeUnit.SECONDS, d.getUnit());

      final TimeDuration sum = d.add(oneSecond);
      assertEquals(10, sum.getDuration());
      assertEquals(TimeUnit.SECONDS, sum.getUnit());
    }
    {
      final TimeDuration d = oneSecond.subtract(tenSecond);
      assertEquals(-9, d.getDuration());
      assertEquals(TimeUnit.SECONDS, d.getUnit());

      final TimeDuration sum = d.add(tenSecond);
      assertEquals(1, sum.getDuration());
      assertEquals(TimeUnit.SECONDS, sum.getUnit());
    }

    final TimeDuration oneMS = TimeDuration.valueOf(1, TimeUnit.MILLISECONDS);
    {
      final TimeDuration d = oneSecond.subtract(oneMS);
      assertEquals(999, d.getDuration());
      assertEquals(TimeUnit.MILLISECONDS, d.getUnit());

      final TimeDuration sum = d.add(oneSecond);
      assertEquals(1999, sum.getDuration());
      assertEquals(TimeUnit.MILLISECONDS, sum.getUnit());
    }
    {
      final TimeDuration d = oneMS.subtract(oneSecond);
      assertEquals(-999, d.getDuration());
      assertEquals(TimeUnit.MILLISECONDS, d.getUnit());

      final TimeDuration sum = d.add(oneSecond);
      assertEquals(1, sum.getDuration());
      assertEquals(TimeUnit.MILLISECONDS, sum.getUnit());
    }
  }

  @Test
  @Timeout(value = 1000)
  public void testNegate() {
    assertNegate(0);
    assertNegate(1);
    assertNegate(-1);
    assertNegate(Long.MAX_VALUE);

    Assertions.assertEquals(
        TimeDuration.valueOf(Long.MAX_VALUE, TimeUnit.SECONDS),
        TimeDuration.valueOf(Long.MIN_VALUE, TimeUnit.SECONDS).negate());
  }

  private static void assertNegate(long n) {
    Assertions.assertEquals(
        TimeDuration.valueOf(-n, TimeUnit.SECONDS),
        TimeDuration.valueOf(n, TimeUnit.SECONDS).negate());
    Assertions.assertEquals(
        TimeDuration.valueOf(n, TimeUnit.SECONDS),
        TimeDuration.valueOf(-n, TimeUnit.SECONDS).negate());
  }

  @Test
  @Timeout(value = 1000)
  public void testMultiply() {
    assertMultiply(0, TimeDuration.ONE_SECOND, TimeDuration.valueOf(0, TimeUnit.SECONDS));
    assertMultiply(0.001, TimeDuration.ONE_SECOND, TimeDuration.ONE_MILLISECOND);
    assertMultiply(0.001/60, TimeDuration.ONE_MINUTE, TimeDuration.ONE_MILLISECOND);
    assertMultiply(100,
        TimeDuration.valueOf(Long.MAX_VALUE / 10, TimeUnit.NANOSECONDS),
        TimeDuration.valueOf(Long.MAX_VALUE / 100, TimeUnit.MICROSECONDS)
    );
    assertMultiply(1E-30,
        TimeDuration.valueOf(1_000_000_000_000_000_000L, TimeUnit.DAYS),
        TimeDuration.valueOf(86, TimeUnit.NANOSECONDS)
    );
  }

  private static void assertMultiply(double multiplier, TimeDuration t, TimeDuration expected) {
    assertMultiply(t         ,  multiplier, expected);
    assertMultiply(t.negate(), -multiplier, expected);
    assertMultiply(t.negate(),  multiplier, expected.negate());
    assertMultiply(t         , -multiplier, expected.negate());
  }

  private static void assertMultiply(TimeDuration t, double multiplier, TimeDuration expected) {
    final TimeDuration computed = t.multiply(multiplier);
    TimeDuration.LOG.info("assertMultiply: {} x {} = {} ?= {}\n\n", t, multiplier, computed, expected);
    Assertions.assertEquals(expected.getUnit(), computed.getUnit());
    final long d = Math.abs(computed.getDuration() - expected.getDuration());
    Assertions.assertTrue(d <= Math.abs(expected.getDuration()) * TimeDuration.ERROR_THRESHOLD);
  }

  @Test
  @Timeout(value = 1000)
  public void testHigherLower() {
    final TimeUnit[] units = {TimeUnit.NANOSECONDS, TimeUnit.MICROSECONDS, TimeUnit.MILLISECONDS,
        TimeUnit.SECONDS, TimeUnit.MINUTES, TimeUnit.HOURS, TimeUnit.DAYS};
    for(int i = 1; i < units.length; i++) {
      assertHigherLower(units[i-1], units[i]);
    }

    Assertions.assertSame(TimeUnit.NANOSECONDS, TimeDuration.lowerUnit(TimeUnit.NANOSECONDS));
    Assertions.assertSame(TimeUnit.DAYS, TimeDuration.higherUnit(TimeUnit.DAYS));
  }

  @Test
  @Timeout(value = 1000)
  public void testCompareTo() {
    assertTimeDurationCompareTo(TimeDuration.ONE_MINUTE, TimeDuration.ONE_SECOND);

    final TimeDuration fifteenSecond = TimeDuration.valueOf(15, TimeUnit.SECONDS);
    assertTimeDurationCompareTo(TimeDuration.ONE_DAY, fifteenSecond);

    assertTimeDurationEquals(TimeDuration.ONE_MINUTE, fifteenSecond.multiply(4));
    assertTimeDurationEquals(TimeDuration.ONE_DAY, TimeDuration.ONE_MINUTE.multiply(60).multiply(24));
  }

  static void assertTimeDurationEquals(TimeDuration left, TimeDuration right) {
    assertEquals(0, left.compareTo(right));
    assertEquals(0, right.compareTo(left));
    assertEquals(left, right);
    assertEquals(right, left);
  }

  static void assertTimeDurationCompareTo(TimeDuration larger, TimeDuration smaller) {
    assertTrue(smaller.compareTo(larger) < 0);
    assertTrue(larger.compareTo(smaller) > 0);
    assertEquals(smaller, TimeDuration.min(smaller, larger));
    assertEquals(smaller, TimeDuration.min(larger, smaller));
    assertEquals(larger, TimeDuration.max(smaller, larger));
    assertEquals(larger, TimeDuration.max(larger, smaller));

    final TimeDuration diff = larger.add(smaller.negate());
    assertTrue(diff.isPositive());
    assertTrue(diff.isNonNegative());
    assertFalse(diff.isNegative());
    assertFalse(diff.isNonPositive());
  }

  private static void assertHigherLower(TimeUnit lower, TimeUnit higher) {
    Assertions.assertSame(lower, TimeDuration.lowerUnit(higher));
    Assertions.assertSame(higher, TimeDuration.higherUnit(lower));
  }
}
