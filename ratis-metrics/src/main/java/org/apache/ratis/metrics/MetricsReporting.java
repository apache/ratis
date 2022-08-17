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
package org.apache.ratis.metrics;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.JmxReporter.Builder;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class MetricsReporting {
  public static final Logger LOG = LoggerFactory.getLogger(MetricsReporting.class);

  private MetricsReporting() {
  }

  public static Consumer<RatisMetricRegistry> consoleReporter(TimeDuration rate) {
    return ratisMetricRegistry -> {
      ConsoleReporter reporter = ConsoleReporter.forRegistry(ratisMetricRegistry.getDropWizardMetricRegistry())
          .convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS).build();
      reporter.start(rate.getDuration(), rate.getUnit());
      ratisMetricRegistry.setConsoleReporter(reporter);
    };
  }

  public static Consumer<RatisMetricRegistry> stopConsoleReporter() {
    return ratisMetricRegistry -> {
      ConsoleReporter reporter = ratisMetricRegistry.getConsoleReporter();
      if (reporter != null) {
        reporter.close();
      }
    };
  }

  public static Consumer<RatisMetricRegistry> jmxReporter() {
    return registry -> {
      Builder builder =
          JmxReporter.forRegistry(registry.getDropWizardMetricRegistry());
      builder.inDomain(registry.getMetricRegistryInfo().getApplicationName());
      JmxReporter reporter = builder.build();
      reporter.start();

      registry.setJmxReporter(reporter);
    };
  }

  public static Consumer<RatisMetricRegistry> stopJmxReporter() {
    return registry -> {
      JmxReporter reporter = registry.getJmxReporter();
      if (reporter != null) {
        reporter.close();
      }
    };
  }
}

