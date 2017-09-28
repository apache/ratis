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
package org.apache.ratis.util;

import org.slf4j.Logger;

/** Facilitates hooking process termination for tests and debugging. */
public class ExitUtils {
  public static class ExitException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public final int status;

    public ExitException(int status, String message, Throwable throwable) {
      super(message, throwable);
      this.status = status;
    }
  }

  private static volatile boolean systemExitDisabled = false;
  private static volatile ExitException firstExitException;

  /**
   * @return the first {@link ExitException} thrown, or null if none thrown yet.
   */
  public static ExitException getFirstExitException() {
    return firstExitException;
  }

  /**
   * Reset the tracking of process termination.
   * This is useful when some tests expect an exit but the others do not.
   */
  public static void resetFirstExitException() {
    firstExitException = null;
  }

  /** @return true if {@link #terminate(int, String, Throwable, Logger)} has been invoked. */
  public static boolean isTerminated() {
    // Either this member is set or System.exit is actually invoked.
    return firstExitException != null;
  }

  public static void assertNotTerminated() {
    if (ExitUtils.isTerminated()) {
      throw new AssertionError("Unexpected exited.", getFirstExitException());
    }
  }

  /** Disable the use of {@link System#exit(int)} for testing. */
  public static void disableSystemExit() {
    systemExitDisabled = true;
  }

  /**
   * Terminate the current process. Note that terminate is the *only* method
   * that should be used to terminate the daemon processes.
   *
   * @param status Exit status
   * @param message message used to create the {@code ExitException}
   * @throws ExitException if System.exit is disabled for test purposes
   */
  public static void terminate(
      int status, String message, Throwable throwable, Logger log)
      throws ExitException {
    if (log != null) {
      final String s = "Terminating with exit status " + status + ": " + message;
      if (status == 0) {
        log.info(s, throwable);
      } else {
        log.error(s, throwable);
      }
    }

    if (!systemExitDisabled) {
      System.exit(status);
    }

    final ExitException ee = new ExitException(status, message, throwable);
    if (firstExitException == null) {
      firstExitException = ee;
    }
    throw ee;
  }

  public static void terminate(int status, String message, Logger log) {
    terminate(status, message, null, log);
  }
}
