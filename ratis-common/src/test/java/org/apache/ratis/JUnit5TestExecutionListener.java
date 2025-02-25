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

import org.apache.ratis.util.JavaUtils;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;

import java.io.PrintStream;
import java.util.concurrent.TimeoutException;

/**
 * A {@link TestExecutionListener} to dump all threads after a test timeout failure.
 */
public class JUnit5TestExecutionListener implements TestExecutionListener {
  private final PrintStream out = System.out;

  @Override
  public void executionFinished(TestIdentifier id, TestExecutionResult result) {
    final Throwable timeoutException = getTimeoutException(result);
    if (timeoutException != null) {
      out.format("%n%s %s.%s failed%n", JavaUtils.date(), id.getClass().getSimpleName(), id.getDisplayName());
      timeoutException.printStackTrace(out);
      JavaUtils.dumpAllThreads(out::println);
    }
  }

  private static Throwable getTimeoutException(TestExecutionResult result) {
    if (result == null) {
      return null;
    }
    final Throwable throwable = result.getThrowable().orElse(null);
    return throwable instanceof TimeoutException? throwable : null;
  }
}
