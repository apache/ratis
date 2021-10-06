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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

public class TestTaskQueue {
  static int randomSleep(int id) {
    final int sleepMs = 10 + ThreadLocalRandom.current().nextInt(10);
    try {
      Thread.sleep(sleepMs);
    } catch (InterruptedException e) {
      throw new IllegalStateException(e);
    }
    return id;
  }

  @Test
  public void testIsEmpty() throws Exception {
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    final TaskQueue q = new TaskQueue("testing");
    for(int i = 0; i < 10; i++) {
      final int id = i;
      q.submit(() -> randomSleep(id), executor)
          .thenAccept(j -> Assert.assertTrue("Queue is not empty after task " + id + " completed", q.isEmpty()))
          .get();
    }
  }
}
