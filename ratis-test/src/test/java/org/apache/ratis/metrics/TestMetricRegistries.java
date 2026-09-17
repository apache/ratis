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
package org.apache.ratis.metrics;

import static org.apache.ratis.metrics.MetricRegistries.DEFAULT_CLASS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.ratis.metrics.impl.MetricRegistriesImpl;
import org.apache.ratis.util.ServiceUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

/**
 * Test class for loading {@link MetricRegistries} using {@link ServiceUtils}.
 */
public class TestMetricRegistries {
  static MetricRegistries load(List<MetricRegistries> loaded) {
    return ServiceUtils.load(MetricRegistries.class, MetricRegistries::global, loaded);
  }

  @Test
  public void testLoadEmptyInstance() {
    MetricRegistries instance = load(Collections.emptyList());
    assertEquals(DEFAULT_CLASS, instance.getClass().getName());
    assertSame(MetricRegistries.global(), instance);
  }

  @Test
  public void testLoadSingleInstance() {
    MetricRegistries loader = mock(MetricRegistries.class);
    MetricRegistries instance = load(Collections.singletonList(loader));
    assertEquals(loader, instance);
  }

  @Test
  public void testLoadMultipleInstances() {
    final MetricRegistries first = mock(MetricRegistries.class);
    final MetricRegistries second = MetricRegistries.global();
    final MetricRegistries instance = load(Arrays.asList(first, second));

    // the load() returns the first instance
    assertEquals(first, instance);
    assertNotEquals(second, instance);
  }

  @Test
  public void testLoadDefault() {
    final MetricRegistries loaded = MetricRegistries.global();
    Assertions.assertSame(MetricRegistriesImpl.class, loaded.getClass());
    Assertions.assertSame(MetricRegistries.global(), loaded);
  }

  @Test
  public void testAddRemoveReporter() {
    final AtomicLong cntr = new AtomicLong(0L);
    final MetricRegistries r = MetricRegistries.global();
    Consumer<RatisMetricRegistry> reporter = v-> cntr.incrementAndGet();
    Consumer<RatisMetricRegistry> stopReporter = v-> cntr.incrementAndGet();
    r.addReporterRegistration(reporter, stopReporter);

    // check if add and remove of metric do reporting counter increase
    MetricRegistryInfo info = new MetricRegistryInfo("t1", "t1", "t1", "t1");
    r.create(info);
    assertEquals(1, cntr.get());
    r.remove(info);
    assertEquals(2, cntr.get());

    // after removal, add and remove of metric must not do any increase
    r.removeReporterRegistration(reporter, stopReporter);
    r.create(info);
    assertEquals(2, cntr.get());
    r.remove(info);
    assertEquals(2, cntr.get());
  }

}
