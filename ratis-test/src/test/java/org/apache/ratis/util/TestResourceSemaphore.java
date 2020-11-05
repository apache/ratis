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

import org.apache.ratis.BaseTest;
import org.apache.ratis.RaftTestUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeoutException;

import static org.apache.ratis.util.ResourceSemaphore.ResourceAcquireStatus.FAILED_IN_BYTE_SIZE_LIMIT;
import static org.apache.ratis.util.ResourceSemaphore.ResourceAcquireStatus.FAILED_IN_ELEMENT_LIMIT;
import static org.apache.ratis.util.ResourceSemaphore.ResourceAcquireStatus.SUCCESS;

public class TestResourceSemaphore extends BaseTest {
  @Test(timeout = 5000)
  public void testGroup() throws InterruptedException, TimeoutException {
    final ResourceSemaphore.Group g = new ResourceSemaphore.Group(3, 1);

    assertUsed(g, 0, 0);
    assertAcquire(g, ResourceSemaphore.ResourceAcquireStatus.SUCCESS, 1, 1);
    assertUsed(g, 1, 1);
    assertAcquire(g, FAILED_IN_BYTE_SIZE_LIMIT, 1, 1);
    assertUsed(g, 1, 1);
    assertAcquire(g, FAILED_IN_BYTE_SIZE_LIMIT, 0, 1);
    assertUsed(g, 1, 1);
    assertAcquire(g, SUCCESS, 1, 0);
    assertUsed(g, 2, 1);
    assertAcquire(g, SUCCESS, 1, 0);
    assertUsed(g, 3, 1);
    assertAcquire(g, FAILED_IN_ELEMENT_LIMIT, 1, 0);
    assertUsed(g, 3, 1);

    g.release(1, 1);
    assertUsed(g, 2, 0);
    g.release(2, 0);
    assertUsed(g, 0, 0);
    g.release(0, 0);
    assertUsed(g, 0, 0);

    g.acquire(1, 1);
    assertUsed(g, 1, 1);
    final Thread t = new Thread(acquire(g, 1, 1));
    t.start();
    RaftTestUtil.waitFor(() -> Thread.State.WAITING == t.getState(), 100, 1000);
    assertUsed(g, 2, 1);

    g.release(0, 1);
    RaftTestUtil.waitFor(() -> Thread.State.TERMINATED == t.getState(), 100, 1000);
    assertUsed(g, 2, 1);

    final Thread t1 = new Thread(acquire(g, 1, 1));
    t1.start();
    RaftTestUtil.waitFor(() -> Thread.State.WAITING == t1.getState(), 100, 1000);
    assertUsed(g, 3, 1);

    t1.interrupt();
    RaftTestUtil.waitFor(() -> Thread.State.TERMINATED == t1.getState(), 100, 1000);
    assertUsed(g, 2, 1);
    g.release(2, 1);

    testFailureCase("release over limit-0", () -> g.release(1, 0), IllegalStateException.class);
    testFailureCase("release over limit-1", () -> g.release(0, 1), IllegalStateException.class);
  }

  static void assertUsed(ResourceSemaphore.Group g, int... expected) {
    Assert.assertEquals(expected.length, g.resourceSize());
    for(int i = 0; i < expected.length; i++) {
      Assert.assertEquals(expected[i], g.get(i).used());
    }
  }

  static void assertAcquire(ResourceSemaphore.Group g, ResourceSemaphore.ResourceAcquireStatus expected,
      int... permits) {
    final ResourceSemaphore.ResourceAcquireStatus computed = g.tryAcquire(permits);
    Assert.assertEquals(expected, computed);
  }

  static Runnable acquire(ResourceSemaphore.Group g, int... permits) {
    return () -> {
      try {
        g.acquire(permits);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        e.printStackTrace();
      }
    };
  }
}
