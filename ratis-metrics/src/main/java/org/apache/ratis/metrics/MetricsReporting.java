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

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ganglia.GangliaReporter;
import com.github.joshelser.dropwizard.metrics.hadoop.HadoopMetrics2Reporter;
import info.ganglia.gmetric4j.gmetric.GMetric;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.impl.MetricsSystemImpl;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsReporting {
  public static final Logger LOG = LoggerFactory.getLogger(MetricsReporting.class);
  private TimeUnit unit;
  private long period;
  private static RatisMetricRegistry jvmRegistry;
  private GMetric ganglia;
  static{
    DefaultMetricsSystem.initialize("ratis");
  }

  public enum MetricReporterType {
    JMX, HADOOP2, CONSOLE, GANGLIA
  }

  public MetricsReporting(long period, TimeUnit unit) {
    this.period = period;
    this.unit = unit;
  }

  public void configureGanglia(String host, int port) throws IOException {
    ganglia = new GMetric(host, port, GMetric.UDPAddressingMode.MULTICAST, 1);

  }

  /**
   * @param registry
   * @param reporting
   * @return
   */
  public boolean startMetricsReporter(RatisMetricRegistry registry, MetricReporterType... reporting) {

    MetricRegistry dropWizardRegistry = registry.getDropWizardMetricRegistry();
    for (MetricReporterType reporter : reporting) {
      try {
        switch (reporter) {
        case CONSOLE:
          ConsoleReporter.forRegistry(dropWizardRegistry)
              .convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS).build()
              .start(period, unit);
          break;
        case JMX:
          JmxReporter.Builder builder =
              JmxReporter.forRegistry(dropWizardRegistry);
          builder.inDomain(registry.getMetricRegistryInfo().getApplicationName());
          builder.build().start();
          break;
        case HADOOP2:
          MetricRegistryInfo info = registry.getMetricRegistryInfo();
          HadoopMetrics2Reporter.forRegistry(dropWizardRegistry)
              .build(DefaultMetricsSystem.instance(), info.getName(), info.getMetricsDescription(),
                  info.getMetricsComponentName()).start(period, unit);
          break;
        case GANGLIA:
          if (ganglia == null) {
            throw new IllegalStateException(
                "Ganglia is not configured!! please configure it first by "
                    + "using MetricReporting#configureGanglia..");
          }
          GangliaReporter.forRegistry(dropWizardRegistry)
              .convertRatesTo(TimeUnit.SECONDS)
              .convertDurationsTo(TimeUnit.MILLISECONDS)
              .build(ganglia);
          break;
        default:
          LOG.warn("Unhandled reporter, " + reporter + " provided.");
          return false;
        }
      } catch (Exception e) {
        return false;
      }
    }
    return true;
  }

}