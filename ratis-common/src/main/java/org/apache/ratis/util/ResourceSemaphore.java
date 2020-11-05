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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A {@link Semaphore} with a limit for a resource.
 *
 * After {@link #close()}, the resource becomes unavailable, i.e. any acquire will not succeed.
 */
public class ResourceSemaphore extends Semaphore {
  private final int limit;
  private final AtomicBoolean reducePermits = new AtomicBoolean();
  private final AtomicBoolean isClosed = new AtomicBoolean();

  public ResourceSemaphore(int limit) {
    super(limit, true);
    Preconditions.assertTrue(limit > 0, () -> "limit = " + limit + " <= 0");
    this.limit = limit;
  }

  @Override
  public void release() {
    release(1);
  }

  @Override
  public void release(int permits) {
    assertRelease(permits);
    super.release(permits);
    assertAvailable();
  }

  private void assertRelease(int toRelease) {
    Preconditions.assertTrue(toRelease >= 0, () -> "toRelease = " + toRelease + " < 0");
    final int available = assertAvailable();
    final int permits = Math.addExact(available, toRelease);
    Preconditions.assertTrue(permits <= limit, () -> "permits = " + permits + " > limit = " + limit);
  }

  private int assertAvailable() {
    final int available = availablePermits();
    Preconditions.assertTrue(available >= 0, () -> "available = " + available + " < 0");
    return available;
  }

  public int used() {
    return limit - availablePermits();
  }

  /** Close the resource. */
  public void close() {
    if (reducePermits.compareAndSet(false, true)) {
      reducePermits(limit);
      isClosed.set(true);
    }
  }

  public boolean isClosed() {
    return isClosed.get();
  }

  @Override
  public String toString() {
    return (isClosed()? "closed/": availablePermits() + "/") + limit;
  }

  /**
   * Track a group of resources with a list of {@link ResourceSemaphore}s.
   */

  public enum ResourceAcquireStatus {
    SUCCESS,
    FAILED_IN_ELEMENT_LIMIT,
    FAILED_IN_BYTE_SIZE_LIMIT
  }

  public static class Group {
    private final List<ResourceSemaphore> resources;

    public Group(int... limits) {
      Preconditions.assertTrue(limits.length >= 1, () -> "limits is empty");
      final List<ResourceSemaphore> list = new ArrayList<>(limits.length);
      for(int limit : limits) {
        list.add(new ResourceSemaphore(limit));
      }
      this.resources = Collections.unmodifiableList(list);
    }

    public int resourceSize() {
      return resources.size();
    }

    protected ResourceSemaphore get(int i) {
      return resources.get(i);
    }

    public ResourceAcquireStatus tryAcquire(int... permits) {
      Preconditions.assertTrue(permits.length == resources.size(),
          () -> "items.length = " + permits.length + " != resources.size() = " + resources.size());
      int i = 0;
      // try acquiring all resources
      for(; i < permits.length; i++) {
        if (!resources.get(i).tryAcquire(permits[i])) {
          break;
        }
      }


      if (i == permits.length) {
        return ResourceAcquireStatus.SUCCESS; // successfully acquired all resources
      }

      ResourceAcquireStatus acquireStatus;
      if (i == 0) {
        acquireStatus =  ResourceAcquireStatus.FAILED_IN_ELEMENT_LIMIT;
      } else {
        acquireStatus =  ResourceAcquireStatus.FAILED_IN_BYTE_SIZE_LIMIT;
      }

      // failed at i, releasing all previous resources
      for(i--; i >= 0; i--) {
        resources.get(i).release(permits[i]);
      }

      return acquireStatus;
    }

    public void acquire(int... permits) throws InterruptedException {
      Preconditions.assertTrue(permits.length == resources.size(),
          () -> "items.length = " + permits.length + " != resources.size() = "
              + resources.size());
      int i = 0;
      try {
        for (; i < permits.length; i++) {
          resources.get(i).acquire(permits[i]);
        }
      } catch (Exception e) {
        for (; --i >= 0;) {
          resources.get(i).release(permits[i]);
        }
        throw e;
      }
    }

    protected void release(int... permits) {
      for(int i = resources.size() - 1; i >= 0; i--) {
        resources.get(i).release(permits[i]);
      }
    }

    public void close() {
      for(int i = resources.size() - 1; i >= 0; i--) {
        resources.get(i).close();
      }
    }

    public boolean isClosed() {
      return resources.get(resources.size() - 1).isClosed();
    }

    @Override
    public String toString() {
      return resources + ",size=" + resources.size();
    }
  }
}
