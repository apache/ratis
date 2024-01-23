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
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.util.ExitUtils;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Slf4jUtils;
import org.apache.ratis.util.StringUtils;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.function.CheckedRunnable;
import org.junit.After;
import org.junit.Rule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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

  public List<RaftPeer> getPeersWithPriority(List<RaftPeer> peers, RaftPeer suggestedLeader) {
    List<RaftPeer> peersWithPriority = new ArrayList<>();
    for (int i = 0; i < peers.size(); i++) {
      RaftPeer peer = peers.get(i);
      final int priority = peer.equals(suggestedLeader)? 2: 1;
      peersWithPriority.add(
          RaftPeer.newBuilder(peer).setPriority(priority).build());
    }
    return peersWithPriority;
  }


  /*
   * Junit 4 reference will be removed and the code will be refactored once
   * all the unit tests are migrated to Junit 5.
   */

  private String testCaseName;

  @BeforeEach
  public void setup(TestInfo testInfo) {
    testCaseName = testInfo.getTestMethod()
        .orElseThrow(() -> new RuntimeException("Exception while getting test name."))
        .getName();
  }

  // @After annotation is retained to support junit 4 tests.
  @After
  @AfterEach
  public void assertNoFailures() {
    final Throwable e = firstException.get();
    if (e != null) {
      throw new IllegalStateException("Failed: first exception was set", e);
    }

    ExitUtils.assertNotTerminated();
  }

  // Retained to support junit 4 tests.
  @Rule
  public final org.junit.rules.Timeout globalTimeout = new org.junit.rules.Timeout(
      getGlobalTimeoutSeconds(), TimeUnit.SECONDS );

  // Retained to support junit 4 tests.
  @Rule
  public final TestName testName = new TestName();

  public int getGlobalTimeoutSeconds() {
    return 100;
  }

  private static final Supplier<File> rootTestDir = JavaUtils.memoize(
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
    return rootTestDir.get();
  }

  public File getClassTestDir() {
    return new File(getRootTestDir(), JavaUtils.getClassSimpleName(getClass()));
  }

  public File getTestDir() {
    // This will work for both junit 4 and 5.
    final String name = testCaseName != null ? testCaseName : testName.getMethodName();
    return new File(getClassTestDir(), name);
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
