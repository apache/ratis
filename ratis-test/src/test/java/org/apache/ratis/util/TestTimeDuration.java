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

import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.ratis.util.TimeDuration.Abbreviation;
import static org.apache.ratis.util.TimeDuration.parse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestTimeDuration {
  @Test(timeout = 1000)
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
            .forEach(s -> assertEquals(s, 0L, parse(s, unit))));
  }

  @Test(timeout = 1000)
  public void testParse() {
    assertEquals(1L, parse("1000000 ns", TimeUnit.MILLISECONDS));
    assertEquals(10L, parse("10000000 nanos", TimeUnit.MILLISECONDS));
    assertEquals(100L, parse("100000000 nanosecond", TimeUnit.MILLISECONDS));
    assertEquals(1000L, parse("1000000000 nanoseconds", TimeUnit.MILLISECONDS));

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

  @Test(timeout = 1000)
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

  @Test(timeout = 1000)
  public void testTo() {
    final TimeDuration oneSecond = TimeDuration.valueOf(1, TimeUnit.SECONDS);
    assertTo(1000, oneSecond, TimeUnit.MILLISECONDS);
    final TimeDuration nanos = assertTo(1_000_000_000, oneSecond, TimeUnit.NANOSECONDS);
    assertTo(1000, nanos, TimeUnit.MILLISECONDS);

    assertTo(0, oneSecond, TimeUnit.MINUTES);
    assertTo(0, nanos, TimeUnit.MINUTES);

    final TimeDuration millis = TimeDuration.valueOf(1_999, TimeUnit.MILLISECONDS);
    assertTo(1, millis, TimeUnit.SECONDS);
    assertTo(0, millis, TimeUnit.MINUTES);
  }

  static TimeDuration assertTo(long expected, TimeDuration timeDuration, TimeUnit toUnit) {
    final TimeDuration computed = timeDuration.to(toUnit);
    assertEquals(expected, computed.getDuration());
    assertEquals(toUnit, computed.getUnit());
    return computed;
  }

  @Test(timeout = 1000)
  public void testMinus() {
    final TimeDuration oneSecond = TimeDuration.valueOf(1, TimeUnit.SECONDS);
    final TimeDuration tenSecond = TimeDuration.valueOf(10, TimeUnit.SECONDS);
    {
      final TimeDuration d = oneSecond.minus(oneSecond);
      assertEquals(0, d.getDuration());
      assertEquals(TimeUnit.SECONDS, d.getUnit());
    }
    {
      final TimeDuration d = tenSecond.minus(oneSecond);
      assertEquals(9, d.getDuration());
      assertEquals(TimeUnit.SECONDS, d.getUnit());
    }
    {
      final TimeDuration d = oneSecond.minus(tenSecond);
      assertEquals(-9, d.getDuration());
      assertEquals(TimeUnit.SECONDS, d.getUnit());
    }

    final TimeDuration oneMS = TimeDuration.valueOf(1, TimeUnit.MILLISECONDS);
    {
      final TimeDuration d = oneSecond.minus(oneMS);
      assertEquals(999, d.getDuration());
      assertEquals(TimeUnit.MILLISECONDS, d.getUnit());
    }
    {
      final TimeDuration d = oneMS.minus(oneSecond);
      assertEquals(-999, d.getDuration());
      assertEquals(TimeUnit.MILLISECONDS, d.getUnit());
    }
  }
}
