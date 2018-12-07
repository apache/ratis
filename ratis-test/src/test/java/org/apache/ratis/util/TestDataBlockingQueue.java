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
package org.apache.ratis.util;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TestDataBlockingQueue {
  static final Logger LOG = LoggerFactory.getLogger(TestDataBlockingQueue.class);

  final SizeInBytes byteLimit = SizeInBytes.valueOf(100);
  final int elementLimit = 10;
  final DataBlockingQueue<Integer> q = new DataBlockingQueue<>(null, byteLimit, elementLimit, Integer::intValue);

  final TimeDuration slow = TimeDuration.valueOf(100, TimeUnit.MILLISECONDS);
  final TimeDuration fast = TimeDuration.valueOf(10, TimeUnit.MILLISECONDS);

  @Test(timeout = 1000)
  public void testElementLimit() {
    TestDataQueue.runTestElementLimit(q);
  }

  @Test(timeout = 1000)
  public void testByteLimit() {
    TestDataQueue.runTestByteLimit(q);
  }

  @Test(timeout = 10_000)
  public void testSlowOfferFastPoll() throws Exception {
    runTestBlockingCalls(slow, fast, q);
  }

  @Test(timeout = 10_000)
  public void testFastOfferSlowPoll() throws Exception {
    runTestBlockingCalls(fast, slow, q);
  }

  static void assertOfferPull(int offering, int polled, int elementLimit) {
    Assert.assertTrue(offering >= polled);
    Assert.assertTrue(offering - polled <= elementLimit + 1);
  }

  static void runTestBlockingCalls(TimeDuration offerSleepTime, TimeDuration pollSleepTime,
      DataBlockingQueue<Integer> q) throws Exception {
    Assert.assertTrue(q.isEmpty());
    ExitUtils.disableSystemExit();
    final int elementLimit = q.getElementLimit();
    final TimeDuration timeout = CollectionUtils.min(offerSleepTime, pollSleepTime);

    final AtomicInteger offeringValue = new AtomicInteger();
    final AtomicInteger polledValue = new AtomicInteger();
    final int endValue = 30;

    final Thread pollThread = new Thread(() -> {
      try {
        for(; polledValue.get() < endValue;) {
          pollSleepTime.sleep();
          final Integer polled = q.poll(timeout);
          if (polled != null) {
            Assert.assertEquals(polledValue.incrementAndGet(), polled.intValue());
            LOG.info("polled {}", polled);
          }
          assertOfferPull(offeringValue.get(), polledValue.get(), elementLimit);
        }
      } catch (Throwable t) {
        ExitUtils.terminate(-2, "pollThread failed", t, null);
      }
    });

    final Thread offerThread = new Thread(() -> {
      try {
        for(offeringValue.incrementAndGet(); offeringValue.get() <= endValue; ) {
          offerSleepTime.sleep();
          final boolean offered = q.offer(offeringValue.get(), timeout);
          if (offered) {
            LOG.info("offered {}", offeringValue.getAndIncrement());
          }
          assertOfferPull(offeringValue.get(), polledValue.get(), elementLimit);
        }
      } catch (Throwable t) {
        ExitUtils.terminate(-1, "offerThread failed", t, null);
      }
    });

    pollThread.start();
    offerThread.start();

    offerThread.join();
    pollThread.join();

    Assert.assertEquals(endValue + 1, offeringValue.get());
    Assert.assertEquals(endValue, polledValue.get());

    Assert.assertTrue(q.isEmpty());
    ExitUtils.assertNotTerminated();
  }
}
