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
package org.apache.ratis.metrics.impl;

import java.util.Map;
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
import com.codahale.metrics.MetricRegistry.MetricSupplier;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.Timer;
import org.apache.ratis.metrics.MetricRegistryInfo;
import org.apache.ratis.metrics.RatisMetricRegistry;
import org.apache.ratis.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * Custom implementation of {@link MetricRegistry}.
 */
public class RatisMetricRegistryImpl implements RatisMetricRegistry {
  private MetricRegistry metricRegistry = new MetricRegistry();

  private final MetricRegistryInfo info;

  private JmxReporter jmxReporter;
  private ConsoleReporter consoleReporter;

  public RatisMetricRegistryImpl(MetricRegistryInfo info) {
    super();
    this.info = info;
  }

  @Override
  public Timer timer(String name) {
    return metricRegistry.timer(getMetricName(name));
  }

  @Override
  public Counter counter(String name) {
    return metricRegistry.counter(getMetricName(name));
  }

  @Override
  public boolean remove(String name) {
    return metricRegistry.remove(getMetricName(name));
  }

  @Override public Gauge gauge(String name, MetricSupplier<Gauge> supplier) {
    return metricRegistry.gauge(getMetricName(name), supplier);
  }

  @Override public Timer timer(String name, MetricSupplier<Timer> supplier) {
    return metricRegistry.timer(getMetricName(name), supplier);
  }

  @Override public SortedMap<String, Gauge> getGauges(MetricFilter filter) {
    return metricRegistry.getGauges(filter);
  }

  @Override public Counter counter(String name, MetricSupplier<Counter> supplier) {
    return metricRegistry.counter(getMetricName(name), supplier);
  }

  @Override public Histogram histogram(String name) {
    return metricRegistry.histogram(getMetricName(name));
  }

   @Override public Meter meter(String name) {
    return metricRegistry.meter(getMetricName(name));
  }

  @Override public Meter meter(String name, MetricSupplier<Meter> supplier) {
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
