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

import org.slf4j.Logger;
import org.slf4j.event.Level;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

public interface Slf4jUtils {
  static void setLogLevel(Logger logger, Level level) {
    final String name = logger.getName();
    final boolean set = Log4jUtils.setLevel(name, level.name());

    final String prefix = set? "Successfully": "Failed to";
    if (LogUtils.LOG.isTraceEnabled()) {
      LogUtils.LOG.trace("", new Throwable(prefix + " set " + name + " log level to " + level));
    } else {
      LogUtils.LOG.info(prefix + " set {} log level to {}", name, level);
    }
  }

  interface Log4jUtils {
    String PACKAGE_NAME = "org.apache.log4j";

    /**
     * Use reflection to invoke setLevel.
     *
     * @param loggerName The logger name.
     * @param levelName The level name.
     * @return true iff the level is successfully set.
     */
    static boolean setLevel(String loggerName, String levelName) {
      final Field level = Level.valueOf(levelName);
      if (level == null) {
        return false;
      }

      try {
        final Object logger = LogManager.getLogger().invoke(null, loggerName);
        Logger.setLevel().invoke(logger, level.get(null));
      } catch (Exception ignored) {
        return false;
      }
      return true;
    }

    interface LogManager {
      String CLASS_NAME = PACKAGE_NAME + ".LogManager";
      Supplier<Method> GET_LOGGER = MemoizedSupplier.valueOf(LogManager::getLoggerImpl);

      static Method getLogger() {
        return GET_LOGGER.get();
      }

      static Method getLoggerImpl() {
        final Class<?> clazz = ReflectionUtils.getClassByNameOrNull(CLASS_NAME);
        if (clazz == null) {
          return null;
        }
        final Class<?>[] argClasses = {String.class};
        try {
          return clazz.getMethod("getLogger", argClasses);
        } catch (Exception e) {
          return null;
        }
      }
    }

    interface Level {
      String CLASS_NAME = PACKAGE_NAME + ".Level";
      ConcurrentMap<String, Field> LEVELS = new ConcurrentHashMap<>();

      static Field valueOf(String level) {
        return LEVELS.computeIfAbsent(level, Level::valueOfImpl);
      }

      static Field valueOfImpl(String level) {
        try {
          final Class<?> clazz = ReflectionUtils.getClassByNameOrNull(CLASS_NAME);
          return clazz.getDeclaredField(level);
        } catch (Exception e) {
          return null;
        }
      }
    }

    interface Logger {
      String CLASS_NAME = PACKAGE_NAME + ".Logger";
      Supplier<Method> SET_LEVEL = MemoizedSupplier.valueOf(Logger::setLevelImpl);

      static Method setLevel() {
        return SET_LEVEL.get();
      }

      static Method setLevelImpl() {
        final Class<?> clazz = ReflectionUtils.getClassByNameOrNull(CLASS_NAME);
        if (clazz == null) {
          return null;
        }
        final Class<?>[] argClasses = {ReflectionUtils.getClassByNameOrNull(Level.CLASS_NAME)};
        try {
          return clazz.getMethod("setLevel", argClasses);
        } catch (Exception e) {
          return null;
        }
      }
    }
  }
}
