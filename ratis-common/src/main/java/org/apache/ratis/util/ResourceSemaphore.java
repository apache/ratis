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

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A {@link Semaphore} with an element limit for tracking resources.
 *
 * After {@link #close()}, the resource becomes unavailable, i.e. any acquire will not succeed.
 */
public class ResourceSemaphore extends Semaphore {
  private final int elementLimit;
  private final AtomicBoolean isClosed = new AtomicBoolean();

  public ResourceSemaphore(int elementLimit) {
    super(elementLimit, true);
    this.elementLimit = elementLimit;
  }

  /** Close the resource. */
  public void close() {
    if (isClosed.compareAndSet(false, true)) {
      reducePermits(elementLimit);
    }
  }

  public boolean isClosed() {
    return isClosed.get();
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + ":elementLimit=" + elementLimit + ",closed?" + isClosed;
  }
}
