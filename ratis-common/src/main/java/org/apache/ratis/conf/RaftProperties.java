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

package org.apache.ratis.conf;

import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.ReflectionUtils;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.StringUtils;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * Provides access to configuration parameters. The current implementation is a
 * simplified version of hadoop's Configuration.
 */
public class RaftProperties {
  private static final Logger LOG = LoggerFactory.getLogger(RaftProperties.class);

  private final ConcurrentMap<String, String> properties = new ConcurrentHashMap<>();

  /** A new configuration. */
  public RaftProperties() {
  }

  /**
   * A new RaftProperties with the same settings cloned from another.
   *
   * @param other the RaftProperties from which to clone settings.
   */
  public RaftProperties(RaftProperties other) {
    this.properties.putAll(other.properties);
  }

  private static final int MAX_SUBST = 20;

  private static final int SUB_START_IDX = 0;
  private static final int SUB_END_IDX = SUB_START_IDX + 1;

  /**
   * This is a manual implementation of the following regex
   * "\\$\\{[^\\}\\$\u0020]+\\}".
   *
   * @param eval a string that may contain variables requiring expansion.
   * @return a 2-element int array res such that
   * eval.substring(res[0], res[1]) is "var" for the left-most occurrence of
   * ${var} in eval. If no variable is found -1, -1 is returned.
   */
  private static int[] findSubVariable(String eval) {
    int[] result = {-1, -1};

    int matchStart;
    int leftBrace;

    // scanning for a brace first because it's less frequent than $
    // that can occur in nested class names
    //
    match_loop:
    for (matchStart = 1, leftBrace = eval.indexOf('{', matchStart);
         // minimum left brace position (follows '$')
         leftBrace > 0
         // right brace of a smallest valid expression "${c}"
         && leftBrace + "{c".length() < eval.length();
         leftBrace = eval.indexOf('{', matchStart)) {
      int matchedLen = 0;
      if (eval.charAt(leftBrace - 1) == '$') {
        int subStart = leftBrace + 1; // after '{'
        for (int i = subStart; i < eval.length(); i++) {
          switch (eval.charAt(i)) {
            case '}':
              if (matchedLen > 0) { // match
                result[SUB_START_IDX] = subStart;
                result[SUB_END_IDX] = subStart + matchedLen;
                break match_loop;
              }
              // fall through to skip 1 char
            case ' ':
            case '$':
              matchStart = i + 1;
              continue match_loop;
            default:
              matchedLen++;
          }
        }
        // scanned from "${"  to the end of eval, and no reset via ' ', '$':
        //    no match!
        break;
      } else {
        // not a start of a variable
        //
        matchStart = leftBrace + 1;
      }
    }
    return result;
  }

  /**
   * Attempts to repeatedly expand the value {@code expr} by replacing the
   * left-most substring of the form "${var}" in the following precedence order
   * <ol>
   *   <li>by the value of the environment variable "var" if defined</li>
   *   <li>by the value of the Java system property "var" if defined</li>
   *   <li>by the value of the configuration key "var" if defined</li>
   * </ol>
   *
   * If var is unbounded the current state of expansion "prefix${var}suffix" is
   * returned.
   *
   * If a cycle is detected: replacing var1 requires replacing var2 ... requires
   * replacing var1, i.e., the cycle is shorter than
   * {@link RaftProperties#MAX_SUBST} then the original expr is returned.
   *
   * @param expr the literal value of a config key
   * @return null if expr is null, otherwise the value resulting from expanding
   * expr using the algorithm above.
   * @throws IllegalArgumentException when more than
   * {@link RaftProperties#MAX_SUBST} replacements are required
   */
  private String substituteVars(String expr) {
    if (expr == null) {
      return null;
    }
    String eval = expr;
    Set<String> evalSet = null;
    for(int s = 0; s < MAX_SUBST; s++) {
      final int[] varBounds = findSubVariable(eval);
      if (varBounds[SUB_START_IDX] == -1) {
        return eval;
      }
      final String var = eval.substring(varBounds[SUB_START_IDX],
          varBounds[SUB_END_IDX]);
      String val = null;
      try {
        if (var.startsWith("env.") && 4 < var.length()) {
          String v = var.substring(4);
          int i = 0;
          for (; i < v.length(); i++) {
            char c = v.charAt(i);
            if (c == ':' && i < v.length() - 1 && v.charAt(i + 1) == '-') {
              val = getenv(v.substring(0, i));
              if (val == null || val.length() == 0) {
                val = v.substring(i + 2);
              }
              break;
            } else if (c == '-') {
              val = getenv(v.substring(0, i));
              if (val == null) {
                val = v.substring(i + 1);
              }
              break;
            }
          }
          if (i == v.length()) {
            val = getenv(v);
          }
        } else {
          val = getProperty(var);
        }
      } catch(SecurityException se) {
        LOG.warn("Unexpected SecurityException in Configuration", se);
      }
      if (val == null) {
        val = getRaw(var);
      }
      if (val == null) {
        return eval; // return literal ${var}: var is unbound
      }

      // prevent recursive resolution
      //
      final int dollar = varBounds[SUB_START_IDX] - "${".length();
      final int afterRightBrace = varBounds[SUB_END_IDX] + "}".length();
      final String refVar = eval.substring(dollar, afterRightBrace);
      if (evalSet == null) {
        evalSet = new HashSet<>();
      }
      if (!evalSet.add(refVar)) {
        return expr; // return original expression if there is a loop
      }

      // substitute
      eval = eval.substring(0, dollar)
             + val
             + eval.substring(afterRightBrace);
    }
    throw new IllegalStateException("Variable substitution depth too large: "
                                    + MAX_SUBST + " " + expr);
  }

  String getenv(String name) {
    return System.getenv(name);
  }

  String getProperty(String key) {
    return System.getProperty(key);
  }

  /**
   * Get the value of the <code>name</code> property, <code>null</code> if
   * no such property exists. If the key is deprecated, it returns the value of
   * the first key which replaces the deprecated key and is not null.
   *
   * Values are processed for <a href="#VariableExpansion">variable expansion</a>
   * before being returned.
   *
   * @param name the property name, will be trimmed before get value.
   * @return the value of the <code>name</code> or its replacing property,
   *         or null if no such property exists.
   */
  public String get(String name) {
    return substituteVars(getRaw(name));
  }

  /**
   * Get the value of the <code>name</code> property as a trimmed <code>String</code>,
   * <code>null</code> if no such property exists.
   * If the key is deprecated, it returns the value of
   * the first key which replaces the deprecated key and is not null
   *
   * Values are processed for <a href="#VariableExpansion">variable expansion</a>
   * before being returned.
   *
   * @param name the property name.
   * @return the value of the <code>name</code> or its replacing property,
   *         or null if no such property exists.
   */
  public String getTrimmed(String name) {
    String value = get(name);

    if (null == value) {
      return null;
    } else {
      return value.trim();
    }
  }

  /**
   * Get the value of the <code>name</code> property, without doing
   * <a href="#VariableExpansion">variable expansion</a>.If the key is
   * deprecated, it returns the value of the first key which replaces
   * the deprecated key and is not null.
   *
   * @param name the property name.
   * @return the value of the <code>name</code> property or
   *         its replacing property and null if no such property exists.
   */
  public String getRaw(String name) {
    return properties.get(Objects.requireNonNull(name, "name == null").trim());
  }

  /**
   * Set the <code>value</code> of the <code>name</code> property. If
   * <code>name</code> is deprecated, it also sets the <code>value</code> to
   * the keys that replace the deprecated key. Name will be trimmed before put
   * into configuration.
   *
   * @param name property name.
   * @param value property value.
   * @throws IllegalArgumentException when the value or name is null.
   */
  public void set(String name, String value) {
    final String trimmed = Objects.requireNonNull(name, "name == null").trim();
    Objects.requireNonNull(value, () -> "value == null for " + trimmed);
    properties.put(trimmed, value);
  }

  /**
   * Unset a previously set property.
   */
  public void unset(String name) {
    properties.remove(name);
  }

  /**
   * Sets a property if it is currently unset.
   * @param name the property name
   * @param value the new value
   */
  public synchronized void setIfUnset(String name, String value) {
    if (get(name) == null) {
      set(name, value);
    }
  }

  /**
   * Get the value of the <code>name</code>. If the key is deprecated,
   * it returns the value of the first key which replaces the deprecated key
   * and is not null.
   * If no such property exists,
   * then <code>defaultValue</code> is returned.
   *
   * @param name property name, will be trimmed before get value.
   * @param defaultValue default value.
   * @return property value, or <code>defaultValue</code> if the property
   *         doesn't exist.
   */
  public String get(String name, String defaultValue) {
    return substituteVars(properties.getOrDefault(name, defaultValue));
  }

  /**
   * Get the value of the <code>name</code> property as an <code>int</code>.
   *
   * If no such property exists, the provided default value is returned,
   * or if the specified value is not a valid <code>int</code>,
   * then an error is thrown.
   *
   * @param name property name.
   * @param defaultValue default value.
   * @throws NumberFormatException when the value is invalid
   * @return property value as an <code>int</code>,
   *         or <code>defaultValue</code>.
   */
  public int getInt(String name, int defaultValue) {
    String valueString = getTrimmed(name);
    if (valueString == null) {
      return defaultValue;
    }
    String hexString = getHexDigits(valueString);
    if (hexString != null) {
      return Integer.parseInt(hexString, 16);
    }
    return Integer.parseInt(valueString);
  }

  /**
   * Set the value of the <code>name</code> property to an <code>int</code>.
   *
   * @param name property name.
   * @param value <code>int</code> value of the property.
   */
  public void setInt(String name, int value) {
    set(name, Integer.toString(value));
  }


  /**
   * Get the value of the <code>name</code> property as a <code>long</code>.
   * If no such property exists, the provided default value is returned,
   * or if the specified value is not a valid <code>long</code>,
   * then an error is thrown.
   *
   * @param name property name.
   * @param defaultValue default value.
   * @throws NumberFormatException when the value is invalid
   * @return property value as a <code>long</code>,
   *         or <code>defaultValue</code>.
   */
  public long getLong(String name, long defaultValue) {
    String valueString = getTrimmed(name);
    if (valueString == null) {
      return defaultValue;
    }
    String hexString = getHexDigits(valueString);
    if (hexString != null) {
      return Long.parseLong(hexString, 16);
    }
    return Long.parseLong(valueString);
  }

  /** @return property value; if it is not set, return the default value. */
  public File getFile(String name, File defaultValue) {
    final String valueString = getTrimmed(name);
    return valueString == null? defaultValue: new File(valueString);
  }

  /**
   * Get the value of the <code>name</code> property as a list
   * of <code>File</code>.
   * The value of the property specifies a list of comma separated path names.
   * If no such property is specified, then <code>defaultValue</code> is
   * returned.
   *
   * @param name the property name.
   * @param defaultValue default value.
   * @return property value as a List of File, or <code>defaultValue</code>.
   */
  public List<File> getFiles(String name, List<File> defaultValue) {
    String valueString = getRaw(name);
    if (null == valueString) {
      return defaultValue;
    }
    String[] paths = getTrimmedStrings(name);
    return Arrays.stream(paths).map(File::new).collect(Collectors.toList());
  }

  public void setFile(String name, File value) {
    try {
      set(name, value.getCanonicalPath());
    } catch (IOException e) {
      throw new IllegalArgumentException(
          "Failed to get canonical path from file " + value + " for " + name, e);
    }
  }

  public void setFiles(String name, List<File> value) {
    String paths = value.stream().map(File::getAbsolutePath)
        .collect(Collectors.joining(","));
    set(name, paths);
  }

  /** @return property value; if it is not set, return the default value. */
  public SizeInBytes getSizeInBytes(String name, SizeInBytes defaultValue) {
    final String valueString = getTrimmed(name);
    return valueString == null? defaultValue: SizeInBytes.valueOf(valueString);
  }

  private String getHexDigits(String value) {
    boolean negative = false;
    String str = value;
    String hexString;
    if (value.startsWith("-")) {
      negative = true;
      str = value.substring(1);
    }
    if (str.startsWith("0x") || str.startsWith("0X")) {
      hexString = str.substring(2);
      if (negative) {
        hexString = "-" + hexString;
      }
      return hexString;
    }
    return null;
  }

  /**
   * Set the value of the <code>name</code> property to a <code>long</code>.
   *
   * @param name property name.
   * @param value <code>long</code> value of the property.
   */
  public void setLong(String name, long value) {
    set(name, Long.toString(value));
  }

  /**
   * Get the value of the <code>name</code> property as a <code>double</code>.
   * If no such property exists, the provided default value is returned,
   * or if the specified value is not a valid <code>double</code>,
   * then an error is thrown.
   *
   * @param name property name.
   * @param defaultValue default value.
   * @throws NumberFormatException when the value is invalid
   * @return property value as a <code>double</code>,
   *         or <code>defaultValue</code>.
   */
  public double getDouble(String name, double defaultValue) {
    String valueString = getTrimmed(name);
    if (valueString == null) {
      return defaultValue;
    }
    return Double.parseDouble(valueString);
  }

  /**
   * Set the value of the <code>name</code> property to a <code>double</code>.
   *
   * @param name property name.
   * @param value property value.
   */
  public void setDouble(String name, double value) {
    set(name,Double.toString(value));
  }

  /**
   * Get the value of the <code>name</code> property as a <code>boolean</code>.
   * If no such property is specified, or if the specified value is not a valid
   * <code>boolean</code>, then <code>defaultValue</code> is returned.
   *
   * @param name property name.
   * @param defaultValue default value.
   * @return property value as a <code>boolean</code>,
   *         or <code>defaultValue</code>.
   */
  public boolean getBoolean(String name, boolean defaultValue) {
    String valueString = getTrimmed(name);
    return StringUtils.string2boolean(valueString, defaultValue);
  }

  /**
   * Set the value of the <code>name</code> property to a <code>boolean</code>.
   *
   * @param name property name.
   * @param value <code>boolean</code> value of the property.
   */
  public void setBoolean(String name, boolean value) {
    set(name, Boolean.toString(value));
  }

  /**
   * Set the value of the <code>name</code> property to the given type. This
   * is equivalent to <code>set(&lt;name&gt;, value.toString())</code>.
   * @param name property name
   * @param value new value
   */
  public <T extends Enum<T>> void setEnum(String name, T value) {
    set(name, value.toString());
  }

  /**
   * Return value matching this enumerated type.
   * Note that the returned value is trimmed by this method.
   * @param name Property name
   * @param defaultValue Value returned if no mapping exists
   * @throws IllegalArgumentException If mapping is illegal for the type
   * provided
   */
  public <T extends Enum<T>> T getEnum(String name, T defaultValue) {
    final String val = getTrimmed(name);
    return null == val
      ? defaultValue
      : Enum.valueOf(defaultValue.getDeclaringClass(), val);
  }

  /**
   * Set the value of <code>name</code> to the given time duration. This
   * is equivalent to <code>set(&lt;name&gt;, value + &lt;time suffix&gt;)</code>.
   * @param name Property name
   * @param value Time duration
   */
  public void setTimeDuration(String name, TimeDuration value) {
    set(name, value.toString());
  }

  /**
   * Return time duration in the given time unit. Valid units are encoded in
   * properties as suffixes: nanoseconds (ns), microseconds (us), milliseconds
   * (ms), seconds (s), minutes (m), hours (h), and days (d).
   * @param name Property name
   * @param defaultValue Value returned if no mapping exists.
   * @throws NumberFormatException If the property stripped of its unit is not
   *         a number
   */
  public TimeDuration getTimeDuration(
      String name, TimeDuration defaultValue, TimeUnit defaultUnit) {
    final String value = getTrimmed(name);
    if (null == value) {
      return defaultValue;
    }
    try {
      return TimeDuration.valueOf(value, defaultUnit);
    } catch(NumberFormatException e) {
      throw new IllegalArgumentException("Failed to parse "
          + name + " = " + value, e);
    }
  }
  public BiFunction<String, TimeDuration, TimeDuration> getTimeDuration(TimeUnit defaultUnit) {
    return (key, defaultValue) -> getTimeDuration(key, defaultValue, defaultUnit);
  }

  /**
   * Get the comma delimited values of the <code>name</code> property as
   * an array of <code>String</code>s, trimmed of the leading and trailing whitespace.
   * If no such property is specified then an empty array is returned.
   *
   * @param name property name.
   * @return property value as an array of trimmed <code>String</code>s,
   *         or empty array.
   */
  public String[] getTrimmedStrings(String name) {
    String valueString = get(name);
    return StringUtils.getTrimmedStrings(valueString);
  }



  /**
   * Get the value of the <code>name</code> property
   * as an array of <code>Class</code>.
   * The value of the property specifies a list of comma separated class names.
   * If no such property is specified, then <code>defaultValue</code> is
   * returned.
   *
   * @param name the property name.
   * @param defaultValue default value.
   * @return property value as a <code>Class[]</code>,
   *         or <code>defaultValue</code>.
   */
  public Class<?>[] getClasses(String name, Class<?> ... defaultValue) {
    String valueString = getRaw(name);
    if (null == valueString) {
      return defaultValue;
    }
    String[] classnames = getTrimmedStrings(name);
    try {
      Class<?>[] classes = new Class<?>[classnames.length];
      for(int i = 0; i < classnames.length; i++) {
        classes[i] = ReflectionUtils.getClassByName(classnames[i]);
      }
      return classes;
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Get the value of the <code>name</code> property as a <code>Class</code>.
   * If no such property is specified, then <code>defaultValue</code> is
   * returned.
   *
   * @param name the class name.
   * @param defaultValue default value.
   * @return property value as a <code>Class</code>,
   *         or <code>defaultValue</code>.
   */
  public Class<?> getClass(String name, Class<?> defaultValue) {
    String valueString = getTrimmed(name);
    if (valueString == null) {
      return defaultValue;
    }
    try {
      return ReflectionUtils.getClassByName(valueString);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Get the value of the <code>name</code> property as a <code>Class</code>
   * implementing the interface specified by <code>xface</code>.
   *
   * If no such property is specified, then <code>defaultValue</code> is
   * returned.
   *
   * An exception is thrown if the returned class does not implement the named
   * interface.
   *
   * @param name the class name.
   * @param defaultValue default value.
   * @param xface the interface implemented by the named class.
   * @return property value as a <code>Class</code>,
   *         or <code>defaultValue</code>.
   */
  public <BASE> Class<? extends BASE> getClass(
      String name, Class<? extends BASE> defaultValue, Class<BASE> xface) {
    try {
      Class<?> theClass = getClass(name, defaultValue);
      if (theClass != null && !xface.isAssignableFrom(theClass)) {
        throw new RuntimeException(theClass+" not "+xface.getName());
      } else if (theClass != null) {
        return theClass.asSubclass(xface);
      } else {
        return null;
      }

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Set the value of the <code>name</code> property to the name of a
   * <code>theClass</code> implementing the given interface <code>xface</code>.
   *
   * An exception is thrown if <code>theClass</code> does not implement the
   * interface <code>xface</code>.
   *
   * @param name property name.
   * @param theClass property value.
   * @param xface the interface implemented by the named class.
   */
  public void setClass(String name, Class<?> theClass, Class<?> xface) {
    if (!xface.isAssignableFrom(theClass)) {
      throw new RuntimeException(theClass+" not "+xface.getName());
    }
    set(name, theClass.getName());
  }

  /** @return number of keys in the properties. */
  public int size() {
    return properties.size();
  }

  /**
   * Clears all keys from the configuration.
   */
  public void clear() {
    properties.clear();
  }

  /** @return the property entry set. */
  Set<Map.Entry<String, String>> entrySet() {
    return properties.entrySet();
  }

  @Override
  public String toString() {
    return JavaUtils.getClassSimpleName(getClass()) + ":" + size();
  }
}
