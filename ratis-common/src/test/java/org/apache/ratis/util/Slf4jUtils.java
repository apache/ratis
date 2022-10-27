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

import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.event.Level;

import java.util.Optional;

public interface Slf4jUtils {

  static void setLogLevel(Logger logger, Level level) {
    final String name = logger.getName();
    if (LogUtils.LOG.isTraceEnabled()) {
      LogUtils.LOG.trace("", new Throwable("Set " + name + " log level to " + level));
    } else {
      LogUtils.LOG.info("Set {} log level to {}", name, level);
    }

    Optional.ofNullable(LogManager.getLogger(name))
        .ifPresent(log -> log.setLevel(getLog4jLevel(level)));
  }

  static org.apache.log4j.Level getLog4jLevel(Level level) {
    switch (level) {
      case ERROR: return org.apache.log4j.Level.ERROR;
      case WARN: return org.apache.log4j.Level.WARN;
      case INFO: return org.apache.log4j.Level.INFO;
      case DEBUG: return org.apache.log4j.Level.DEBUG;
      case TRACE: return org.apache.log4j.Level.TRACE;
    }
    throw new IllegalArgumentException("Unexpected level " + level);
  }
}
