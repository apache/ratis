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


import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.ratis.conf.RaftProperties;

/**
 * Reflection related utility methods.
 */
public class ReflectionUtils {

  private ReflectionUtils() {
    // Utility class, cannot instantiate
  }

  private static final Class<?>[] EMPTY_CLASSES = {};

  /**
   * Cache of constructors for each class. Pins the classes so they
   * can't be garbage collected until ReflectionUtils can be collected.
   */
  private static final Map<List<Class<?>>, Constructor<?>> CONSTRUCTOR_CACHE
      = new ConcurrentHashMap<>();

  /**
   * Create an object for the given class using its default constructor.
   */
  public static <T> T newInstance(Class<T> clazz) {
    return newInstance(clazz, EMPTY_CLASSES);
  }

  /**
   * Create an object for the given class using the specified constructor.
   *
   * @param clazz class of which an object is created
   * @param argClasses argument classes of the constructor
   * @param args actual arguments to be passed to the constructor
   * @param <T> class type of clazz
   * @return a new object
   */
  public static <T> T newInstance(Class<T> clazz, Class<?>[] argClasses, Object... args) {
    Objects.requireNonNull(clazz, "clazz == null");
    try {
      final List<Class<?>> key = new ArrayList<>();
      key.add(clazz);
      key.addAll(Arrays.asList(argClasses));

      @SuppressWarnings("unchecked")
      Constructor<T> ctor = (Constructor<T>) CONSTRUCTOR_CACHE.get(key);
      if (ctor == null) {
        ctor = clazz.getDeclaredConstructor(argClasses);
        ctor.setAccessible(true);
        CONSTRUCTOR_CACHE.put(key, ctor);
      }
      return instantiate(clazz.getName(), ctor, args);
    } catch (NoSuchMethodException e) {
      throw new UnsupportedOperationException(
          "Unable to find suitable constructor for class " + clazz.getName(), e);
    }
  }

  private static <T> T instantiate(final String className, Constructor<T> ctor, Object[] ctorArgs) {
    try {
      ctor.setAccessible(true);
      return ctor.newInstance(ctorArgs);
    } catch (IllegalAccessException e) {
      throw new UnsupportedOperationException(
          "Unable to access specified class " + className, e);
    } catch (InstantiationException e) {
      throw new UnsupportedOperationException(
          "Unable to instantiate specified class " + className, e);
    } catch (InvocationTargetException e) {
      throw new UnsupportedOperationException(
          "Constructor threw an exception for " + className, e);
    }
  }

  /** Is the given object an instance of one of the given classes? */
  public static boolean isInstance(Object obj, Class<?>... classes) {
    for(Class<?> c : classes) {
      if (c.isInstance(obj)) {
        return true;
      }
    }
    return false;
  }

  public static <BASE> Class<? extends BASE> getClass(
      String subClassName, RaftProperties properties, Class<BASE> base) {
    try {
      return properties.getClassByName(subClassName).asSubclass(base);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Failed to get class "
          + subClassName + " as a subclass of " + base, e);
    }
  }

  public static Exception instantiateException(Class<? extends Exception> cls,
      String message, Exception from) throws Exception {
    Constructor<? extends Exception> cn = cls.getConstructor(String.class);
    cn.setAccessible(true);
    Exception ex = cn.newInstance(message);
    if (from != null) {
      ex.initCause(from);
    }
    return ex;
  }
}
