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
package org.apache.ratis;

import org.apache.log4j.Level;
import org.apache.ratis.conf.ConfUtils;
import org.apache.ratis.util.CheckedRunnable;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.LogUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

public abstract class BaseTest {
  public final Logger LOG = LoggerFactory.getLogger(getClass());

  {
    LogUtils.setLogLevel(ConfUtils.LOG, Level.WARN);
  }

  @Rule
  public final Timeout globalTimeout = new Timeout(getGlobalTimeoutMs());

  public int getGlobalTimeoutMs() {
    return 100_000;
  }

  private static final Supplier<File> rootTestDir = JavaUtils.memoize(
      () -> JavaUtils.callAsUnchecked(() -> {
        final File dir = new File(System.getProperty("test.build.data", "target/test/data"),
            Long.toHexString(ThreadLocalRandom.current().nextLong()));
        if (dir.exists() && !dir.isDirectory()) {
          throw new IOException(dir + " already exists and is not a directory");
        } else if (!dir.exists() && !dir.mkdirs()) {
          throw new IOException("Cannot create test directory " + dir);
        }
        return dir;
      }));


  public static File getRootTestDir() {
    return rootTestDir.get();
  }

  public static File getClassTestDir(Class<?> caller) {
    return new File(getRootTestDir(), caller.getSimpleName());
  }

  public File getTestDir() {
    return getClassTestDir(getClass());
  }

  public static void testFailureCase(
      String description, CheckedRunnable<?> testCode,
      Class<? extends Throwable> exceptedThrowableClass, Logger log) {
    try {
      testCode.run();
      Assert.fail("The test \"" + description + "\" does not throw anything.");
    } catch (Throwable t) {
      log.info("The test \"" + description + "\" throws " + t.getClass().getSimpleName(), t);
      Assert.assertEquals(exceptedThrowableClass, t.getClass());
    }
  }

  public void testFailureCase(
      String description, CheckedRunnable<?> testCode, Class<? extends Throwable> exceptedThrowableClass) {
    testFailureCase(description, testCode, exceptedThrowableClass, LOG);
  }
}
