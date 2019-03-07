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
import org.junit.internal.runners.statements.FailOnTimeout;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;
import org.junit.runners.model.Statement;
import org.junit.runners.model.TestTimedOutException;

import java.io.PrintStream;
import java.util.concurrent.TimeUnit;

/**
 * A {@link RunListener} to dump all threads after a test timeout failure.
 */
public class JUnitRunListener extends RunListener {
  private static final Throwable TIMEOUT_EXCEPTION = getTimeoutException();
  private static final String TIMEOUT_EXCEPTION_PREFIX;

  private static Throwable getTimeoutException() {
    final FailOnTimeout f = FailOnTimeout.builder().withTimeout(1, TimeUnit.NANOSECONDS).build(new Statement() {
      @Override
      public void evaluate() throws InterruptedException {
        Thread.sleep(1000);
      }
    });
    try {
      f.evaluate();
    } catch(Throwable throwable) {
      return throwable;
    }
    throw new IllegalStateException("Failed to getTimeoutException");
  }

  static {
    final String message = JUnitRunListener.TIMEOUT_EXCEPTION.getMessage();
    TIMEOUT_EXCEPTION_PREFIX = message.substring(0, message.indexOf('1'));
  }

  private final PrintStream out = System.out;

  @Override
  public void testFailure(Failure failure) {
    final Throwable timeoutException = getTimeoutException(failure);
    if (timeoutException != null) {
      out.format("%n%s ", JavaUtils.date());
      timeoutException.printStackTrace(out);
      JavaUtils.dumpAllThreads(out::println);
    }
  }

  private static Throwable getTimeoutException(Failure failure) {
    if (failure == null) {
      return null;
    }
    final Throwable throwable = failure.getException();
    if (throwable.getClass() != TIMEOUT_EXCEPTION.getClass()) {
      return null;
    }
    final String message = throwable.getMessage();
    if (message == null || !message.startsWith(TIMEOUT_EXCEPTION_PREFIX)) {
      return null;
    }
    return throwable;
  }

  public static void main(String[] args) {
    final JUnitRunListener listener = new JUnitRunListener();
    listener.out.println("TIMEOUT_EXCEPTION_PREFIX = '" + TIMEOUT_EXCEPTION_PREFIX + "'");
    TIMEOUT_EXCEPTION.printStackTrace(listener.out);

    listener.testFailure(new Failure(null, new TestTimedOutException(999, TimeUnit.MILLISECONDS)));
  }
}
