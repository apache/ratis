/*
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

import org.apache.ratis.metrics.RatisMetricRegistry;
import org.apache.ratis.thirdparty.com.codahale.metrics.ConsoleReporter;
import org.apache.ratis.thirdparty.com.codahale.metrics.ScheduledReporter;
import org.apache.ratis.thirdparty.com.codahale.metrics.jmx.JmxReporter;
import org.apache.ratis.util.TimeDuration;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

final class MetricsReporting {
  private MetricsReporting() {
  }

  static Consumer<RatisMetricRegistry> consoleReporter(TimeDuration rate) {
    return registry -> consoleReporter(rate, registry);
  }

  private static void consoleReporter(TimeDuration rate, RatisMetricRegistry registry) {
    final RatisMetricRegistryImpl impl = RatisMetricRegistryImpl.cast(registry);
    final ConsoleReporter reporter = ConsoleReporter.forRegistry(impl.getDropWizardMetricRegistry())
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .build();
    reporter.start(rate.getDuration(), rate.getUnit());
    impl.setConsoleReporter(reporter);
  }

  static Consumer<RatisMetricRegistry> stopConsoleReporter() {
    return MetricsReporting::stopConsoleReporter;
  }

  private static void stopConsoleReporter(RatisMetricRegistry registry) {
    final RatisMetricRegistryImpl impl = RatisMetricRegistryImpl.cast(registry);
    Optional.ofNullable(impl.getConsoleReporter()).ifPresent(ScheduledReporter::close);
  }

  static Consumer<RatisMetricRegistry> jmxReporter() {
    return MetricsReporting::jmxReporter;
  }

  private static void jmxReporter(RatisMetricRegistry registry) {
    final RatisMetricRegistryImpl impl = RatisMetricRegistryImpl.cast(registry);
    final JmxReporter reporter = JmxReporter.forRegistry(impl.getDropWizardMetricRegistry())
        .inDomain(registry.getMetricRegistryInfo().getApplicationName())
        .createsObjectNamesWith(new RatisObjectNameFactory())
        .build();
    reporter.start();
    impl.setJmxReporter(reporter);
  }


  static Consumer<RatisMetricRegistry> stopJmxReporter() {
    return MetricsReporting::stopJmxReporter;
  }

  private static void stopJmxReporter(RatisMetricRegistry registry) {
    final RatisMetricRegistryImpl impl = RatisMetricRegistryImpl.cast(registry);
    Optional.ofNullable(impl.getJmxReporter()).ifPresent(JmxReporter::close);
  }
}
