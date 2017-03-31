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
import java.util.function.Supplier;

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
   * Create an object for the given class using its default constructor.
   */
  public static <T> T newInstance(String className) {
    return newInstance(className, EMPTY_CLASSES);
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

  @SuppressWarnings("unchecked")
  public static <T> T newInstance(String className, Class<? >[] argClasses,
                                        Object... args) {
    try {
      Class<? extends T> resultType = (Class<? extends T>) Class.forName(className);
      return newInstance(resultType, argClasses, args);
    } catch (ClassNotFoundException e) {
      throw new UnsupportedOperationException(
          "Unable to find " + className, e);
    }
  }

  public static <T> T newInstance(Class<T> type, Object... params) {
    return newInstance(type, findConstructor(type, params), params);
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

  @SuppressWarnings("unchecked")
  public static <T> Constructor<T> findConstructor(Class<T> type, Object... paramTypes) {
    Constructor<T>[] constructors = (Constructor<T>[]) type.getDeclaredConstructors();
    for (Constructor<T> ctor : constructors) {
      Class<?>[] ctorParamTypes = ctor.getParameterTypes();
      if (ctorParamTypes.length != paramTypes.length) {
        continue;
      }

      boolean match = true;
      for (int i = 0; i < ctorParamTypes.length && match; ++i) {
        Class<?> paramType = paramTypes[i].getClass();
        match = (!ctorParamTypes[i].isPrimitive()) ? ctorParamTypes[i].isAssignableFrom(paramType) :
            ((int.class.equals(ctorParamTypes[i]) && Integer.class.equals(paramType)) ||
                (long.class.equals(ctorParamTypes[i]) && Long.class.equals(paramType)) ||
                (double.class.equals(ctorParamTypes[i]) && Double.class.equals(paramType)) ||
                (char.class.equals(ctorParamTypes[i]) && Character.class.equals(paramType)) ||
                (short.class.equals(ctorParamTypes[i]) && Short.class.equals(paramType)) ||
                (boolean.class.equals(ctorParamTypes[i]) && Boolean.class.equals(paramType)) ||
                (byte.class.equals(ctorParamTypes[i]) && Byte.class.equals(paramType)));
      }

      if (match) {
        return ctor;
      }
    }
    throw new UnsupportedOperationException(
        "Unable to find suitable constructor for class " + type.getName());
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

  /**
   * Create a memoized supplier which gets a value by invoking the initializer once
   * and then keeps returning the same value as its supplied results.
   *
   * @param initializer to supply at most one non-null value.
   * @param <T> The supplier result type.
   * @return a memoized supplier which is thread-safe.
   */
  public static <T> Supplier<T> memoize(Supplier<T> initializer) {
    Objects.requireNonNull(initializer, "initializer == null");
    return new Supplier<T>() {
      private volatile T value = null;

      @Override
      public T get() {
        T v = value;
        if (v == null) {
          synchronized (this) {
            v = value;
            if (v == null) {
              v = value = Objects.requireNonNull(initializer.get(),
                  "initializer.get() returns null");
            }
          }
        }
        return v;
      }
    };
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
