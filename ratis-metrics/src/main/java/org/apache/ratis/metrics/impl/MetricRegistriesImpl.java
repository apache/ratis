/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ratis.metrics.impl;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import org.apache.ratis.metrics.MetricRegistries;
import org.apache.ratis.metrics.MetricRegistryFactory;
import org.apache.ratis.metrics.MetricRegistryInfo;
import org.apache.ratis.metrics.MetricsReporting;
import org.apache.ratis.metrics.RatisMetricRegistry;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of MetricRegistries that does ref-counting.
 */
public class MetricRegistriesImpl extends MetricRegistries {

  private static final Logger LOG = LoggerFactory.getLogger(MetricRegistriesImpl.class);

  private final List<Consumer<RatisMetricRegistry>> reporterRegistrations = new CopyOnWriteArrayList<>();

  private final List<Consumer<RatisMetricRegistry>> stopReporters = new CopyOnWriteArrayList<>();

  private final MetricRegistryFactory factory;

  private final RefCountingMap<MetricRegistryInfo, RatisMetricRegistry> registries;

  public MetricRegistriesImpl() {
    this(new MetricRegistryFactoryImpl());
  }

  public MetricRegistriesImpl(MetricRegistryFactory factory) {
    this.factory = factory;
    this.registries = new RefCountingMap<>();
  }

  @Override
  public RatisMetricRegistry create(MetricRegistryInfo info) {
    return registries.put(info, () -> {
      if (reporterRegistrations.isEmpty()) {
        LOG.warn(
            "First MetricRegistry has been created without registering reporters. You may need to call" +
                " MetricRegistries.global().addReporterRegistration(...) before.");
      }
      RatisMetricRegistry registry = factory.create(info);
      reporterRegistrations.forEach(reg -> reg.accept(registry));
      return registry;
    });
  }

  @Override
  public boolean remove(MetricRegistryInfo key) {
    RatisMetricRegistry registry = registries.get(key);
    if (registry != null) {
      stopReporters.forEach(reg -> reg.accept(registry));
    }

    return registries.remove(key) == null;
  }

  @Override
  public Optional<RatisMetricRegistry> get(MetricRegistryInfo info) {
    return Optional.ofNullable(registries.get(info));
  }

  @Override
  public Collection<RatisMetricRegistry> getMetricRegistries() {
    return Collections.unmodifiableCollection(registries.values());
  }

  @Override
  public void clear() {
    registries.clear();
  }

  @Override
  public Set<MetricRegistryInfo> getMetricRegistryInfos() {
    return Collections.unmodifiableSet(registries.keySet());
  }

  @Override
  public void addReporterRegistration(Consumer<RatisMetricRegistry> reporterRegistration,
      Consumer<RatisMetricRegistry> stopReporter) {
    this.reporterRegistrations.add(reporterRegistration);
    this.stopReporters.add(stopReporter);
  }

  @Override
  public void enableJmxReporter() {
    addReporterRegistration(
        MetricsReporting.jmxReporter(),
        MetricsReporting.stopJmxReporter());
  }

  @Override
  public void enableConsoleReporter(TimeDuration consoleReportRate) {
    addReporterRegistration(
        MetricsReporting.consoleReporter(consoleReportRate),
        MetricsReporting.stopConsoleReporter());
  }
}
