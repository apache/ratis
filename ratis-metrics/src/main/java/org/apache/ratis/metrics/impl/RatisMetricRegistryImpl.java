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
package org.apache.ratis.metrics.impl;

import org.apache.ratis.metrics.LongCounter;
import org.apache.ratis.metrics.MetricRegistryInfo;
import org.apache.ratis.metrics.RatisMetricRegistry;
import org.apache.ratis.metrics.Timekeeper;
import org.apache.ratis.thirdparty.com.codahale.metrics.ConsoleReporter;
import org.apache.ratis.thirdparty.com.codahale.metrics.Counter;
import org.apache.ratis.thirdparty.com.codahale.metrics.Gauge;
import org.apache.ratis.thirdparty.com.codahale.metrics.Histogram;
import org.apache.ratis.thirdparty.com.codahale.metrics.Meter;
import org.apache.ratis.thirdparty.com.codahale.metrics.Metric;
import org.apache.ratis.thirdparty.com.codahale.metrics.MetricFilter;
import org.apache.ratis.thirdparty.com.codahale.metrics.MetricRegistry;
import org.apache.ratis.thirdparty.com.codahale.metrics.MetricSet;
import org.apache.ratis.thirdparty.com.codahale.metrics.jmx.JmxReporter;
import org.apache.ratis.thirdparty.com.google.common.annotations.VisibleForTesting;

import java.util.Map;
import java.util.SortedMap;
import java.util.function.Supplier;

/**
 * Custom implementation of {@link MetricRegistry}.
 */
public class RatisMetricRegistryImpl implements RatisMetricRegistry {
  private final MetricRegistry metricRegistry = new MetricRegistry();

  private final MetricRegistryInfo info;

  private JmxReporter jmxReporter;
  private ConsoleReporter consoleReporter;

  public RatisMetricRegistryImpl(MetricRegistryInfo info) {
    super();
    this.info = info;
  }

  @Override
  public Timekeeper timer(String name) {
    return new DefaultTimekeeperImpl(metricRegistry.timer(getMetricName(name)));
  }

  static LongCounter toLongCounter(Counter c) {
    return new LongCounter() {
      @Override
      public void inc(long n) {
        c.inc(n);
      }

      @Override
      public void dec(long n) {
        c.dec(n);
      }

      @Override
      public long getCount() {
        return c.getCount();
      }
    };
  }

  @Override
  public LongCounter counter(String name) {
    return toLongCounter(metricRegistry.counter(getMetricName(name)));
  }

  @Override
  public boolean remove(String name) {
    return metricRegistry.remove(getMetricName(name));
  }

  static <T> Gauge<T> toGauge(Supplier<T> supplier) {
    return supplier::get;
  }

  @Override
  public <T> void gauge(String name, Supplier<Supplier<T>> gaugeSupplier) {
    metricRegistry.gauge(getMetricName(name), () -> toGauge(gaugeSupplier.get()));
  }

  public SortedMap<String, Gauge> getGauges(MetricFilter filter) {
    return metricRegistry.getGauges(filter);
  }

  @Override public Histogram histogram(String name) {
    return metricRegistry.histogram(getMetricName(name));
  }

   @Override public Meter meter(String name) {
    return metricRegistry.meter(getMetricName(name));
  }

  @Override public Meter meter(String name, MetricRegistry.MetricSupplier<Meter> supplier) {
    return metricRegistry.meter(getMetricName(name), supplier);
  }

  @Override @VisibleForTesting
  public Metric get(String shortName) {
    return metricRegistry.getMetrics().get(getMetricName(shortName));
  }

  private String getMetricName(String shortName) {
    return MetricRegistry.name(info.getName(), shortName);
  }

  @Override public <T extends Metric> T register(String name, T metric) throws IllegalArgumentException {
    return metricRegistry.register(getMetricName(name), metric);
  }


  @Override public MetricRegistry getDropWizardMetricRegistry() {
    return metricRegistry;
  }

  @Override public MetricRegistryInfo getMetricRegistryInfo(){
    return this.info;
  }

  @Override public void registerAll(String prefix, MetricSet metricSet) {
    for (Map.Entry<String, Metric> entry : metricSet.getMetrics().entrySet()) {
      if (entry.getValue() instanceof MetricSet) {
        registerAll(prefix + "." + entry.getKey(), (MetricSet) entry.getValue());
      } else {
        register(prefix + "." + entry.getKey(), entry.getValue());
      }
    }
  }

  @Override
  public void setJmxReporter(JmxReporter jmxReporter) {
    this.jmxReporter = jmxReporter;
  }

  @Override
  public JmxReporter getJmxReporter() {
    return this.jmxReporter;
  }

  @Override
  public void setConsoleReporter(ConsoleReporter consoleReporter) {
    this.consoleReporter = consoleReporter;
  }

  @Override
  public ConsoleReporter getConsoleReporter() {
    return this.consoleReporter;
  }
}
