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
package org.apache.ratis.metrics;

import java.util.SortedMap;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.Timer;
import org.apache.ratis.thirdparty.com.google.common.annotations.VisibleForTesting;

public interface RatisMetricRegistry {
  Timer timer(String name);

  Counter counter(String name);

  boolean remove(String name);

  Gauge gauge(String name, MetricRegistry.MetricSupplier<Gauge> supplier);

  Timer timer(String name, MetricRegistry.MetricSupplier<Timer> supplier);

  SortedMap<String, Gauge> getGauges(MetricFilter filter);

  Counter counter(String name, MetricRegistry.MetricSupplier<Counter> supplier);

  Histogram histogram(String name);

  Meter meter(String name);

  Meter meter(String name, MetricRegistry.MetricSupplier<Meter> supplier);

  @VisibleForTesting Metric get(String shortName);

  <T extends Metric> T register(String name, T metric) throws IllegalArgumentException;

  MetricRegistry getDropWizardMetricRegistry();

  MetricRegistryInfo getMetricRegistryInfo();

  void registerAll(String prefix, MetricSet metricSet);

  void setJmxReporter(JmxReporter jmxReporter);

  JmxReporter getJmxReporter();

  void setConsoleReporter(ConsoleReporter consoleReporter);

  ConsoleReporter getConsoleReporter();
}
