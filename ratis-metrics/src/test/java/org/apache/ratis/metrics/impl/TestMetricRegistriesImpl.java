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

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.ratis.metrics.MetricRegistries;
import org.apache.ratis.metrics.MetricRegistryInfo;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Supplier;

public class TestMetricRegistriesImpl {
  @Test
  public void testTreadSafe() throws InterruptedException {
    int numberOfThreads = 10;
    int repeatTimes = 10;
    ExecutorService service = Executors.newFixedThreadPool(numberOfThreads);
    MetricRegistryInfo info = Mockito.mock(MetricRegistryInfo.class);
    CyclicBarrier cyclicBarrier = new CyclicBarrier(numberOfThreads);
    List<Future<?>> tasks = new LinkedList<>();

    for (int times = 0; times < repeatTimes; times++) {
      MetricRegistries mockMetricRegistries = spy(new MetricRegistriesImpl());
      for (int i = 0; i < numberOfThreads; i++) {
        Future<?> task = service.submit(() -> {
          try {
            cyclicBarrier.await();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          mockMetricRegistries.create(info);
        });
        tasks.add(task);
      }
      tasks.forEach(task -> {
        try {
          task.get();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });

      verify(mockMetricRegistries, times(1)).enableJmxReporter();
    }

  }
}
