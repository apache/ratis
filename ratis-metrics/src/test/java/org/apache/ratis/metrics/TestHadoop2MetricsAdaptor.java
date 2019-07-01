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

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Counter;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.junit.Test;

public class TestHadoop2MetricsAdaptor {

  @Test public void testMetrics() throws InterruptedException {
    MetricRegistryInfo info =
        new MetricRegistryInfo(TestHadoop2MetricsAdaptor.class.getName(), "ratis_test",
            "test","ratis test metrics");
    RatisMetricRegistry registry = MetricRegistries.global().create(info);
    MetricsReporting metricsReporting = new MetricsReporting(500, TimeUnit.MILLISECONDS);
    metricsReporting
        .startMetricsReporter(registry, MetricsReporting.MetricReporterType.HADOOP2);
    Counter counter = registry.counter("test");
    counter.inc();
    counter.inc();
    counter.inc();
    counter.dec();
    int count = 0;
    Map<String, Long> expectedMetrics = new HashMap<String, Long>();
    expectedMetrics.put("org.apache.ratis.metrics.TestHadoop2MetricsAdaptor.ratis_test.test", 2L);
    Thread.sleep(1000);
    boolean result=false;
    while ( count < 10) {
      if(TestHadoop2MetricsSink.metrics != null) {
        result = verifyMetric(expectedMetrics);
        if (result) {
          break;
        }
      }
      Thread.sleep(1000);
      count++;
    } assertTrue(result);
  }

  public boolean verifyMetric(Map<String, Long> expectedMetrics) {
    for (AbstractMetric metric : TestHadoop2MetricsSink.metrics) {
      if (expectedMetrics.containsKey(metric.name())) {
        long expectedValue = expectedMetrics.get(metric.name());
        long actualValue = metric.value().longValue();
        if (expectedValue != actualValue) {
          return false;
        }
      }
    }
    return true;
  }

}
