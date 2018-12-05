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

import org.junit.Test;

import java.sql.Time;
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
  public void testTimeDuration() {
    Arrays.asList(TimeUnit.values())
        .forEach(a -> assertNotNull(Abbreviation.valueOf(a.name())));
    assertEquals(TimeUnit.values().length, Abbreviation.values().length);

    final List<String> allSymbols = Arrays.asList(Abbreviation.values()).stream()
        .map(Abbreviation::getSymbols)
        .flatMap(List::stream)
        .collect(Collectors.toList());
    Arrays.asList(TimeUnit.values()).forEach(unit ->
        allSymbols.stream()
            .map(s -> "0" + s)
            .forEach(s -> assertEquals(s, 0L, parse(s, unit))));

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
    assertEquals(-nanosPerSecond, oneSecond.roundUp(-nanosPerSecond - 1));
    assertEquals(-nanosPerSecond, oneSecond.roundUp(-nanosPerSecond));
    assertEquals(0, oneSecond.roundUp(-nanosPerSecond + 1));
    assertEquals(0, oneSecond.roundUp(-1));
    assertEquals(0, oneSecond.roundUp(0));
    assertEquals(nanosPerSecond, oneSecond.roundUp(1));
    assertEquals(nanosPerSecond, oneSecond.roundUp(nanosPerSecond - 1));
    assertEquals(nanosPerSecond, oneSecond.roundUp(nanosPerSecond));
    assertEquals(2*nanosPerSecond, oneSecond.roundUp(nanosPerSecond + 1));
  }
}
