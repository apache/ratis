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
package org.apache.ratis.metrics.dropwizard3;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.ratis.metrics.MetricRegistries;
import org.apache.ratis.metrics.MetricRegistriesLoader;
import org.apache.ratis.metrics.MetricRegistryInfo;
import org.apache.ratis.metrics.RatisMetricRegistry;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test class for {@link MetricRegistriesLoader}.
 */
public class TestLoadDm3MetricRegistries {
  @Test
  public void testLoadDm3() {
    final MetricRegistries r = MetricRegistriesLoader.load();
    Assert.assertSame(Dm3MetricRegistriesImpl.class, r.getClass());
  }

  @Test
  public void testAddRemoveReporter() {
    final AtomicLong cntr = new AtomicLong(0L);
    final MetricRegistries r = Dm3MetricRegistriesImpl.global();
    Consumer<RatisMetricRegistry> reporter = v-> cntr.incrementAndGet();
    Consumer<RatisMetricRegistry> stopReporter = v-> cntr.incrementAndGet();
    r.addReporterRegistration(reporter, stopReporter);

    // check if add and remove of metric do reporting counter increase
    MetricRegistryInfo info = new MetricRegistryInfo("t1", "t1", "t1", "t1");
    r.create(info);
    Assert.assertTrue(cntr.get() == 1);
    r.remove(info);
    Assert.assertTrue(cntr.get() == 2);

    // after removal, add and remove of metric must not do any increase
    r.removeReporterRegistration(reporter, stopReporter);
    r.create(info);
    Assert.assertTrue(cntr.get() == 2);
    r.remove(info);
    Assert.assertTrue(cntr.get() == 2);
  }
}
