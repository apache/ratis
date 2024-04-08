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
import org.apache.ratis.util.ExitUtils.ExitException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class TestExitUtils extends BaseTest {
  /** Test if {@link BaseTest} can handle uncaught exception. */
  @Test
  @Timeout(value = 1000)
  public void testUncaughtException() throws Exception {
    Assertions.assertFalse(ExitUtils.isTerminated());
    Assertions.assertFalse(ExitUtils.clear());

    final Thread t = new Thread(null, () -> {
      throw new AssertionError("Testing");
    }, "testThread");
    t.start();
    t.join();

    Assertions.assertTrue(ExitUtils.isTerminated());
    Assertions.assertTrue(ExitUtils.clear());
  }

  /** Test if {@link BaseTest} can handle ExitUtils.terminate(..). */
  @Test
  @Timeout(value = 1000)
  public void testExitStatus() {
    Assertions.assertFalse(ExitUtils.isTerminated());
    Assertions.assertFalse(ExitUtils.clear());

    final int status = -1;
    try {
      ExitUtils.terminate(status, "testExitStatus", LOG);
      Assertions.fail();
    } catch (ExitException e) {
      Assertions.assertEquals(status, e.getStatus());
    }

    Assertions.assertTrue(ExitUtils.isTerminated());
    Assertions.assertTrue(ExitUtils.clear());
  }
}
