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
package org.apache.ratis;

import org.apache.ratis.conf.ConfUtils;
import org.apache.ratis.util.ExitUtils;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.ReferenceCountedLeakDetector;
import org.apache.ratis.util.Slf4jUtils;
import org.apache.ratis.util.StringUtils;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.function.CheckedRunnable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

@Timeout(value = 100)
public abstract class BaseTest {
  public final Logger LOG = LoggerFactory.getLogger(getClass());

  public static final TimeDuration HUNDRED_MILLIS = TimeDuration.valueOf(100, TimeUnit.MILLISECONDS);
  public static final TimeDuration ONE_SECOND = TimeDuration.ONE_SECOND;
  public static final TimeDuration FIVE_SECONDS = TimeDuration.valueOf(5, TimeUnit.SECONDS);

  {
    Slf4jUtils.setLogLevel(ConfUtils.LOG, Level.WARN);
    Slf4jUtils.setLogLevel(FileUtils.LOG, Level.TRACE);

    ExitUtils.disableSystemExit();
  }

  private final AtomicReference<Throwable> firstException = new AtomicReference<>();

  public void setFirstException(Throwable e) {
    if (firstException.compareAndSet(null, e)) {
      LOG.error("Set firstException", e);
    }
  }

  // TODO: Junit 4 reference should be removed once all the unit tests are migrated to Junit 5.

  private String testCaseName;

  @BeforeEach
  public void setup(TestInfo testInfo) {
    checkAssumptions();

    final Method method = testInfo.getTestMethod().orElse(null);
    testCaseName = testInfo.getTestClass().orElse(getClass()).getSimpleName()
        + "." + (method == null? null : method.getName());
  }

  @BeforeEach
  public void checkAssumptions() {
    final int leaks = ReferenceCountedLeakDetector.getLeakDetector().getLeakCount();
    Assumptions.assumeFalse(0 < leaks, () -> "numLeaks " + leaks + " > 0");

    final Throwable first = firstException.get();
    Assumptions.assumeTrue(first == null, () -> "Already failed with " + first);

    final Throwable exited = ExitUtils.getFirstExitException();
    Assumptions.assumeTrue(exited == null, () -> "Already exited with " + exited);
  }

  @AfterEach
  public void assertNoFailures() {
    final Throwable e = firstException.get();
    if (e != null) {
      throw new IllegalStateException("Failed: first exception was set", e);
    }

    ExitUtils.assertNotTerminated();
  }

  private static final Supplier<File> ROOT_TEST_DIR = JavaUtils.memoize(
      () -> JavaUtils.callAsUnchecked(() -> {
        final File dir = new File(System.getProperty("test.build.data", "target/test/data"),
            Integer.toHexString(ThreadLocalRandom.current().nextInt()));
        if (dir.exists() && !dir.isDirectory()) {
          throw new IOException(dir + " already exists and is not a directory");
        } else if (!dir.exists() && !dir.mkdirs()) {
          throw new IOException("Cannot create test directory " + dir);
        }
        return dir;
      }));


  public static File getRootTestDir() {
    return ROOT_TEST_DIR.get();
  }

  public File getClassTestDir() {
    return new File(getRootTestDir(), JavaUtils.getClassSimpleName(getClass()));
  }

  public File getTestDir() {
    // This will work for both junit 4 and 5.
    return new File(getClassTestDir(), testCaseName);
  }

  @SafeVarargs
  public static void assertThrowable(
      String description, Throwable t,
      Class<? extends Throwable> expectedThrowableClass, Logger log,
      Class<? extends Throwable>... expectedCauseClasses) {
    if (log != null) {
      log.info("Expected the test \"{}\" to throw {} with cause(s) {}",
          description, expectedThrowableClass.getSimpleName(),
          StringUtils.array2String(expectedCauseClasses, Class::getSimpleName));
    }
    Assertions.assertEquals(expectedThrowableClass, t.getClass());

    for (Class<? extends Throwable> expectedCause : expectedCauseClasses) {
      final Throwable previous = t;
      t = Objects.requireNonNull(previous.getCause(),
          () -> "previous.getCause() == null for previous=" + previous);
      Assertions.assertEquals(expectedCause, t.getClass());
    }
  }

  @SafeVarargs
  public static Throwable testFailureCase(
      String description, CheckedRunnable<?> testCode,
      Class<? extends Throwable> expectedThrowableClass, Logger log,
      Class<? extends Throwable>... expectedCauseClasses) {
    if (log != null) {
      log.info("run '{}'", description);
    }
    try {
      testCode.run();
    } catch (Throwable t) {
      assertThrowable(description, t, expectedThrowableClass, log, expectedCauseClasses);
      return t;
    }
    throw new AssertionError("The test \"" + description + "\" does not throw anything.");
  }

  @SafeVarargs
  public final Throwable testFailureCase(
      String description, CheckedRunnable<?> testCode,
      Class<? extends Throwable> expectedThrowableClass,
      Class<? extends Throwable>... expectedCauseClasses) {
    return testFailureCase(description, testCode, expectedThrowableClass, LOG, expectedCauseClasses);
  }

  @SafeVarargs
  public static Throwable testFailureCaseAsync(
      String description, Supplier<CompletableFuture<?>> testCode,
      Class<? extends Throwable> expectedThrowableClass, Logger log,
      Class<? extends Throwable>... expectedCauseClasses) {
    if (log != null) {
      log.info("run '{}'", description);
    }
    try {
      testCode.get().join();
    } catch (Throwable t) {
      t = JavaUtils.unwrapCompletionException(t);
      assertThrowable(description, t, expectedThrowableClass, log, expectedCauseClasses);
      return t;
    }
    throw new AssertionError("The test \"" + description + "\" does not throw anything.");
  }

  @SafeVarargs
  public final Throwable testFailureCaseAsync(
      String description, Supplier<CompletableFuture<?>> testCode, Class<? extends Throwable> expectedThrowableClass,
      Class<? extends Throwable>... expectedCauseClasses) {
    return testFailureCaseAsync(description, testCode, expectedThrowableClass, LOG, expectedCauseClasses);
  }

  static <T> T getWithDefaultTimeout(Future<T> future) throws Exception {
    return future.get(FIVE_SECONDS.getDuration(), FIVE_SECONDS.getUnit());
  }
}
