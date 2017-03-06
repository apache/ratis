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
package org.apache.ratis.conf;

import org.apache.ratis.util.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;

public interface ConfUtils {
  Logger LOG = LoggerFactory.getLogger(ConfUtils.class);

  static void logGet(String key, Object value) {
    LOG.info("{} = {}", key, value);
  }

  static void logSet(String key, Object value) {
    LOG.debug("set {} = {}", key, value);
  }

  static BiConsumer<String, Integer> requireMin(int min) {
    return (key, value) -> {
      if (value < min) {
        throw new IllegalArgumentException(
            key + " = " + value + " < min = " + min);
      }
    };
  }

  static BiConsumer<String, Integer> requireMax(int max) {
    return (key, value) -> {
      if (value > max) {
        throw new IllegalArgumentException(
            key + " = " + value + " > max = " + max);
      }
    };
  }

  static BiConsumer<String, Long> requireMin(long min) {
    return (key, value) -> {
      if (value < min) {
        throw new IllegalArgumentException(
            key + " = " + value + " < min = " + min);
      }
    };
  }

  static BiConsumer<String, Long> requireMax(long max) {
    return (key, value) -> {
      if (value > max) {
        throw new IllegalArgumentException(
            key + " = " + value + " > max = " + max);
      }
    };
  }

  static boolean getBoolean(
      BiFunction<String, Boolean, Boolean> booleanGetter,
      String key, boolean defaultValue, BiConsumer<String, Boolean>... assertions) {
    return get(booleanGetter, key, defaultValue, assertions);
  }

  static int getInt(
      BiFunction<String, Integer, Integer> integerGetter,
      String key, int defaultValue, BiConsumer<String, Integer>... assertions) {
    return get(integerGetter, key, defaultValue, assertions);
  }

  static long getLong(
      BiFunction<String, Long, Long> longGetter,
      String key, long defaultValue, BiConsumer<String, Long>... assertions) {
    return get(longGetter, key, defaultValue, assertions);
  }

  static <T> T get(BiFunction<String, T, T> getter,
      String key, T defaultValue, BiConsumer<String, T>... assertions) {
    final T value = getter.apply(key, defaultValue);
    logGet(key, value);
    Arrays.asList(assertions).forEach(a -> a.accept(key, value));
    return value;
  }

  static InetSocketAddress getInetSocketAddress(
      BiFunction<String, String, String> stringGetter,
      String key, String defaultValue) {
    return NetUtils.createSocketAddr(get(stringGetter, key, defaultValue));
  }

  static void setBoolean(
      BiConsumer<String, Boolean> booleanSetter, String key, boolean value,
      BiConsumer<String, Boolean>... assertions) {
    set(booleanSetter, key, value, assertions);
  }

  static void setInt(
      BiConsumer<String, Integer> integerSetter, String key, int value,
      BiConsumer<String, Integer>... assertions) {
    set(integerSetter, key, value, assertions);
  }

  static void setLong(
      BiConsumer<String, Long> longSetter, String key, long value,
      BiConsumer<String, Long>... assertions) {
    set(longSetter, key, value, assertions);
  }

  static <T> void set(
      BiConsumer<String, T> setter, String key, T value,
      BiConsumer<String, T>... assertions) {
    Arrays.asList(assertions).forEach(a -> a.accept(key, value));
    setter.accept(key, value);
    logSet(key, value);
  }

  static void printAll(Class<?> confClass) {
    ConfUtils.printAll(confClass, System.out::println);
  }

  static void printAll(Class<?> confClass, Consumer<Object> out) {
    out.accept("");
    out.accept("******* " + confClass + " *******");
    Arrays.asList(confClass.getDeclaredFields())
        .forEach(f -> printField(confClass, out, f));
    Arrays.asList(confClass.getClasses())
        .forEach(c -> printAll(c, s -> out.accept("  " + s)));
  }

  static void printField(Class<?> confClass, Consumer<Object> out, Field f) {
    if (!Modifier.isStatic(f.getModifiers())) {
      out.accept("WARNING: Found non-static field " + f);
      return;
    }
    final String fieldName = f.getName();
    if (!fieldName.endsWith("_KEY")) {
      if (!fieldName.endsWith("_DEFAULT")) {
        try {
          out.accept("constant: " + fieldName + " = " + f.get(null));
        } catch (IllegalAccessException e) {
          out.accept(fieldName + " is not public");
        }
      }
      return;
    }

    final StringBuilder b = new StringBuilder();
    try {
      final Object keyName = f.get(null);
      b.append("key: ").append(keyName);
    } catch (IllegalAccessException e) {
      out.accept("WARNING: Failed to access key " + f);
      b.append(fieldName + " is not public");
    }
    final String defaultFieldName = fieldName.substring(0, fieldName.length() - 4) + "_DEFAULT";
    b.append(" (");
    try {
      final Field defaultField = confClass.getDeclaredField(defaultFieldName);
      b.append(defaultField.getType().getSimpleName()).append(", ");

      final Object defaultValue = defaultField.get(null);
      b.append("default=").append(defaultValue);
    } catch (NoSuchFieldException e) {
      out.accept("WARNING: Default value not found for field " + f);
      b.append("default not found");
    } catch (IllegalAccessException e) {
      out.accept("WARNING: Failed to access default value " + f);
      b.append(defaultFieldName).append(" is not public");
    }
    b.append(")");
    out.accept(b);
  }
}
