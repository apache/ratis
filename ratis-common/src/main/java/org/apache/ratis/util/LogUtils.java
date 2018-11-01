/*
 * *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.ratis.util;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Logging (as in log4j) related utility methods.
 */
public interface LogUtils {
  Logger LOG = LoggerFactory.getLogger(LogUtils.class);

  static void setLogLevel(Logger logger, Level level) {
    final String name = logger.getName();
    if (LOG.isTraceEnabled()) {
      LOG.trace("", new Throwable("Set " + name + " log level to " + level));
    } else {
      LOG.info("Set {} log level to {}", name, level);
    }
    LogManager.getLogger(name).setLevel(level);
  }

  static <THROWABLE extends Throwable> void runAndLog(
      Logger log, CheckedRunnable<THROWABLE> op, Supplier<String> opName)
      throws THROWABLE {
    try {
      op.run();
    } catch (Throwable t) {
      if (log.isTraceEnabled()) {
        log.trace("Failed to " + opName.get(), t);
      } else if (log.isWarnEnabled()){
        log.warn("Failed to " + opName.get() + ": " + t);
      }
      throw t;
    }

    if (log.isTraceEnabled()) {
      log.trace("Successfully ran " + opName.get());
    }
  }

  static <OUTPUT, THROWABLE extends Throwable> OUTPUT supplyAndLog(
      Logger log, CheckedSupplier<OUTPUT, THROWABLE> supplier, Supplier<String> name)
      throws THROWABLE {
    final OUTPUT output;
    try {
      output = supplier.get();
    } catch (Throwable t) {
      if (log.isTraceEnabled()) {
        log.trace("Failed to " + name.get(), t);
      } else if (log.isWarnEnabled()){
        log.warn("Failed to " + name.get() + ": " + t);
      }
      final THROWABLE throwable = JavaUtils.cast(t);
      throw throwable;
    }

    if (log.isTraceEnabled()) {
      log.trace("Successfully supplied " + name.get() + ": " + output);
    }
    return output;
  }

  static Runnable newRunnable(Logger log, Runnable runnable, Supplier<String> name) {
    return new Runnable() {
      @Override
      public void run() {
        runAndLog(log, runnable::run, name);
      }

      @Override
      public String toString() {
        return name.get();
      }
    };
  }

  static <T> Callable<T> newCallable(Logger log, Callable<T> callable, Supplier<String> name) {
    return new Callable<T>() {
      @Override
      public T call() throws Exception {
        return supplyAndLog(log, callable::call, name);
      }

      @Override
      public String toString() {
        return name.get();
      }
    };
  }

  static <OUTPUT, THROWABLE extends Throwable> CheckedSupplier<OUTPUT, THROWABLE> newCheckedSupplier(
      Logger log, CheckedSupplier<OUTPUT, THROWABLE> supplier, Supplier<String> name) {
    return new CheckedSupplier<OUTPUT, THROWABLE>() {
      @Override
      public OUTPUT get() throws THROWABLE {
        return supplyAndLog(log, supplier, name);
      }

      @Override
      public String toString() {
        return name.get();
      }
    };
  }

  static void warn(Logger log, Supplier<String> message, Throwable t, Class<?>... exceptionClasses) {
    if (log.isWarnEnabled()) {
      if (ReflectionUtils.isInstance(t, exceptionClasses)) {
        // do not print stack trace for known exceptions.
        log.warn(message.get() + ": " + t);
      } else {
        log.warn(message.get(), t);
      }
    }
  }
}
