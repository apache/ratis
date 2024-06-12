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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class TestDataBlockingQueue {
  static final Logger LOG = LoggerFactory.getLogger(TestDataBlockingQueue.class);

  final SizeInBytes byteLimit = SizeInBytes.valueOf(100);
  final int elementLimit = 10;
  final DataBlockingQueue<Long> q =
      new DataBlockingQueue<>(null, byteLimit, elementLimit, Long::longValue);

  final TimeDuration slow = TimeDuration.valueOf(100, TimeUnit.MILLISECONDS);
  final TimeDuration fast = TimeDuration.valueOf(10, TimeUnit.MILLISECONDS);

  @Test
  @Timeout(value = 1)
  public void testElementLimit() {
    TestDataQueue.runTestElementLimit(q);
  }

  @Test
  @Timeout(value = 1)
  public void testByteLimit() {
    TestDataQueue.runTestByteLimit(q);
  }

  @Test
  @Timeout(value = 10)
  public void testSlowOfferFastPoll() throws Exception {
    runTestBlockingCalls(slow, fast, q);
  }

  @Test
  @Timeout(value = 10)
  public void testFastOfferSlowPoll() throws Exception {
    runTestBlockingCalls(fast, slow, q);
  }

  static void assertOfferPull(long offering, long polled, long elementLimit) {
    Assertions.assertTrue(offering >= polled);
    Assertions.assertTrue(offering - polled <= elementLimit + 1);
  }

  static void runTestBlockingCalls(TimeDuration offerSleepTime, TimeDuration pollSleepTime,
      DataBlockingQueue<Long> q) throws Exception {
    Assertions.assertTrue(q.isEmpty());
    ExitUtils.disableSystemExit();
    final int elementLimit = q.getElementLimit();
    final TimeDuration timeout = CollectionUtils.min(offerSleepTime, pollSleepTime);

    final AtomicLong offeringValue = new AtomicLong();
    final AtomicLong polledValue = new AtomicLong();
    final int endValue = 30;

    final Thread pollThread = new Thread(() -> {
      try {
        while (polledValue.get() < endValue) {
          pollSleepTime.sleep();
          final Long polled = q.poll(timeout);
          if (polled != null) {
            Assertions.assertEquals(polledValue.incrementAndGet(), polled.intValue());
            LOG.info("polled {}", polled);
          }
          assertOfferPull(offeringValue.get(), polledValue.get(), elementLimit);
        }
      } catch (Exception e) {
        ExitUtils.terminate(-2, "pollThread failed", e, null);
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
      } catch (Exception e) {
        ExitUtils.terminate(-1, "offerThread failed", e, null);
      }
    });

    pollThread.start();
    offerThread.start();

    offerThread.join();
    pollThread.join();

    Assertions.assertEquals(endValue + 1, offeringValue.get());
    Assertions.assertEquals(endValue, polledValue.get());

    Assertions.assertTrue(q.isEmpty());
    ExitUtils.assertNotTerminated();
  }
}
