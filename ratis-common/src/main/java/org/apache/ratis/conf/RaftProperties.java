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

import org.apache.ratis.util.ReflectionUtils;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.StringUtils;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Attr;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

/**
 * Provides access to configuration parameters. The current implementation is a
 * simplified version of hadoop's Configuration.
 */
public class RaftProperties {
  private static final Logger LOG = LoggerFactory.getLogger(RaftProperties.class);

  private static class Resource {
    private final Object resource;
    private final String name;

    Resource(Object resource) {
      this(resource, resource.toString());
    }

    Resource(Object resource, String name) {
      this.resource = resource;
      this.name = name;
    }

    public String getName(){
      return name;
    }

    public Object getResource() {
      return resource;
    }

    @Override
    public String toString() {
      return name;
    }
  }

  /**
   * List of configuration resources.
   */
  private final ArrayList<Resource> resources;

  /**
   * The value reported as the setting resource when a key is set
   * by code rather than a file resource by dumpConfiguration.
   */
  static final String UNKNOWN_RESOURCE = "Unknown";

  /**
   * List of configuration parameters marked <b>final</b>.
   */
  private final Set<String> finalParameters = Collections.newSetFromMap(new ConcurrentHashMap<>());

  private final boolean loadDefaults;

  /**
   * Configuration objects
   */
  private static final WeakHashMap<RaftProperties, Object> REGISTRY = new WeakHashMap<>();

  /**
   * List of default Resources. Resources are loaded in the order of the list
   * entries
   */
  private static final CopyOnWriteArrayList<String> DEFAULT_RESOURCES =
    new CopyOnWriteArrayList<>();

  /**
   * Stores the mapping of key to the resource which modifies or loads
   * the key most recently
   */
  private final Map<String, String[]> updatingResource;

  private Properties properties;
  private Properties overlay;

  /** A new configuration. */
  public RaftProperties() {
    this(true);
  }

  /** A new configuration where the behavior of reading from the default
   * resources can be turned off.
   *
   * If the parameter {@code loadDefaults} is false, the new instance
   * will not load resources from the default files.
   * @param loadDefaults specifies whether to load from the default files
   */
  public RaftProperties(boolean loadDefaults) {
    this.loadDefaults = loadDefaults;
    this.resources = new ArrayList<>();
    this.updatingResource = new ConcurrentHashMap<>();
    synchronized(RaftProperties.class) {
      REGISTRY.put(this, null);
    }
  }

  /**
   * A new RaftProperties with the same settings cloned from another.
   *
   * @param other the RaftProperties from which to clone settings.
   */
  @SuppressWarnings("unchecked")
  public RaftProperties(RaftProperties other) {
    this.resources = (ArrayList<Resource>) other.resources.clone();
    synchronized(other) {
      if (other.properties != null) {
        this.properties = (Properties)other.properties.clone();
      }

      if (other.overlay!=null) {
        this.overlay = (Properties)other.overlay.clone();
      }

      this.updatingResource = new ConcurrentHashMap<>(other.updatingResource);
      this.finalParameters.addAll(other.finalParameters);
    }

    synchronized(RaftProperties.class) {
      REGISTRY.put(this, null);
    }
    this.loadDefaults = other.loadDefaults;
  }

  /**
   * Add a default resource. Resources are loaded in the order of the resources
   * added.
   * @param name file name. File should be present in the classpath.
   */
  public static synchronized void addDefaultResource(String name) {
    if(!DEFAULT_RESOURCES.contains(name)) {
      DEFAULT_RESOURCES.add(name);
      REGISTRY.keySet().stream().filter(conf -> conf.loadDefaults)
          .forEach(RaftProperties::reloadConfiguration);
    }
  }

  /**
   * Add a configuration resource.
   *
   * The properties of this resource will override properties of previously
   * added resources, unless they were marked <a href="#Final">final</a>.
   *
   * @param name resource to be added, the classpath is examined for a file
   *             with that name.
   */
  public void addResource(String name) {
    addResourceObject(new Resource(name));
  }


  public void addResource(URL path) {
    addResourceObject(new Resource(path));
  }

  /**
   * Add a configuration resource.
   *
   * The properties of this resource will override properties of previously
   * added resources, unless they were marked <a href="#Final">final</a>.
   *
   * WARNING: The contents of the InputStream will be cached, by this method.
   * So use this sparingly because it does increase the memory consumption.
   *
   * @param in InputStream to deserialize the object from. In will be read from
   * when a get or set is called next.  After it is read the stream will be
   * closed.
   */
  public void addResource(InputStream in) {
    addResourceObject(new Resource(in));
  }

  /**
   * Add a configuration resource.
   *
   * The properties of this resource will override properties of previously
   * added resources, unless they were marked <a href="#Final">final</a>.
   *
   * @param in InputStream to deserialize the object from.
   * @param name the name of the resource because InputStream.toString is not
   * very descriptive some times.
   */
  public void addResource(InputStream in, String name) {
    addResourceObject(new Resource(in, name));
  }

  /**
   * Add a configuration resource.
   *
   * The properties of this resource will override properties of previously
   * added resources, unless they were marked <a href="#Final">final</a>.
   *
   * @param conf Configuration object from which to load properties
   */
  public void addResource(RaftProperties conf) {
    addResourceObject(new Resource(conf.getProps()));
  }



  /**
   * Reload configuration from previously added resources.
   *
   * This method will clear all the configuration read from the added
   * resources, and final parameters. This will make the resources to
   * be read again before accessing the values. Values that are added
   * via set methods will overlay values read from the resources.
   */
  public synchronized void reloadConfiguration() {
    properties = null;                            // trigger reload
    finalParameters.clear();                      // clear site-limits
  }

  private synchronized void addResourceObject(Resource resource) {
    resources.add(resource);                      // add to resources
    reloadConfiguration();
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
   * Get the value of the <code>name</code> property as a trimmed <code>String</code>,
   * <code>defaultValue</code> if no such property exists.
   * See @{Configuration#getTrimmed} for more details.
   *
   * @param name          the property name.
   * @param defaultValue  the property default value.
   * @return              the value of the <code>name</code> or defaultValue
   *                      if it is not set.
   */
  public String getTrimmed(String name, String defaultValue) {
    String ret = getTrimmed(name);
    return ret == null ? defaultValue : ret;
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
    return getProps().getProperty(name.trim());
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
    final String trimmed = Objects.requireNonNull(name, "Property name must be non-null.");
    Objects.requireNonNull(value, () -> "The value of property " + trimmed + " must be non-null.");
    name = trimmed;
    getProps();

    getOverlay().setProperty(name, value);
    getProps().setProperty(name, value);
  }

  /**
   * Unset a previously set property.
   */
  public synchronized void unset(String name) {
    getOverlay().remove(name);
    getProps().remove(name);
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

  private synchronized Properties getOverlay() {
    if (overlay == null){
      overlay = new Properties();
    }
    return overlay;
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
    return substituteVars(getProps().getProperty(name, defaultValue));
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
   * Set the given property, if it is currently unset.
   * @param name property name
   * @param value new value
   */
  public void setBooleanIfUnset(String name, boolean value) {
    setIfUnset(name, Boolean.toString(value));
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
   * Get the value of the <code>name</code> property as a <code>Pattern</code>.
   * If no such property is specified, or if the specified value is not a valid
   * <code>Pattern</code>, then <code>DefaultValue</code> is returned.
   * Note that the returned value is NOT trimmed by this method.
   *
   * @param name property name
   * @param defaultValue default value
   * @return property value as a compiled Pattern, or defaultValue
   */
  public Pattern getPattern(String name, Pattern defaultValue) {
    String valString = get(name);
    if (null == valString || valString.isEmpty()) {
      return defaultValue;
    }
    try {
      return Pattern.compile(valString);
    } catch (PatternSyntaxException pse) {
      LOG.warn("Regular expression '" + valString + "' for property '" +
               name + "' not valid. Using default", pse);
      return defaultValue;
    }
  }

  /**
   * Set the given property to <code>Pattern</code>.
   * If the pattern is passed as null, sets the empty pattern which results in
   * further calls to getPattern(...) returning the default value.
   *
   * @param name property name
   * @param pattern new value
   */
  public void setPattern(String name, Pattern pattern) {
    assert pattern != null : "Pattern cannot be null";
    set(name, pattern.pattern());
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

  protected synchronized Properties getProps() {
    if (properties == null) {
      properties = new Properties();
      Map<String, String[]> backup =
          new ConcurrentHashMap<>(updatingResource);
      loadResources();

      if (overlay != null) {
        properties.putAll(overlay);
        for (Entry<Object,Object> item: overlay.entrySet()) {
          String key = (String) item.getKey();
          String[] source = backup.get(key);
          if(source != null) {
            updatingResource.put(key, source);
          }
        }
      }
    }
    return properties;
  }

  /**
   * Return the number of keys in the configuration.
   *
   * @return number of keys in the configuration.
   */
  public int size() {
    return getProps().size();
  }

  /**
   * Clears all keys from the configuration.
   */
  public void clear() {
    getProps().clear();
    getOverlay().clear();
  }

  private Document parse(DocumentBuilder builder, URL url)
      throws IOException, SAXException {
    LOG.debug("parsing URL " + url);
    if (url == null) {
      return null;
    }

    URLConnection connection = url.openConnection();
    if (connection instanceof JarURLConnection) {
      // Disable caching for JarURLConnection to avoid sharing JarFile
      // with other users.
      connection.setUseCaches(false);
    }
    return parse(builder, connection.getInputStream(), url.toString());
  }

  private Document parse(DocumentBuilder builder, InputStream is,
      String systemId) throws IOException, SAXException {
    LOG.debug("parsing input stream " + is);
    if (is == null) {
      return null;
    }
    try {
      return (systemId == null) ? builder.parse(is) : builder.parse(is,
          systemId);
    } finally {
      is.close();
    }
  }

  private void loadResources() {
    if(loadDefaults) {
      for (String resource : DEFAULT_RESOURCES) {
        loadResource(properties, new Resource(resource));
      }
    }

    for (int i = 0; i < resources.size(); i++) {
      Resource ret = loadResource(properties, resources.get(i));
      if (ret != null) {
        resources.set(i, ret);
      }
    }
  }

  private Resource loadResource(Properties propts, Resource wrapper) {
    String name = UNKNOWN_RESOURCE;
    try {
      Object resource = wrapper.getResource();
      name = wrapper.getName();

      DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
      docBuilderFactory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
      docBuilderFactory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
      docBuilderFactory.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
      //ignore all comments inside the xml file
      docBuilderFactory.setIgnoringComments(true);

      //allow includes in the xml file
      docBuilderFactory.setNamespaceAware(true);
      try {
          docBuilderFactory.setXIncludeAware(true);
      } catch (UnsupportedOperationException e) {
        LOG.error("Failed to set setXIncludeAware(true) for parser " + docBuilderFactory + ":" + e, e);
      }
      DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
      Document doc = null;
      Element root = null;
      boolean returnCachedProperties = false;

      if (resource instanceof URL) { // an URL resource
        doc = parse(builder, (URL) resource);
      } else if (resource instanceof String) { // a CLASSPATH resource
        URL url = ReflectionUtils.getClassLoader().getResource((String)resource);
        doc = parse(builder, url);
      } else if (resource instanceof InputStream) {
        doc = parse(builder, (InputStream) resource, null);
        returnCachedProperties = true;
      } else if (resource instanceof Properties) {
        overlay(propts, (Properties) resource);
      } else if (resource instanceof Element) {
        root = (Element) resource;
      }

      if (root == null) {
        if (doc == null) {
          return null;
        }
        root = doc.getDocumentElement();
      }
      Properties toAddTo = propts;
      if(returnCachedProperties) {
        toAddTo = new Properties();
      }
      if (!"configuration".equals(root.getTagName())) {
        LOG.error("bad conf file: top-level element not <configuration>");
      }
      NodeList props = root.getChildNodes();
      for (int i = 0; i < props.getLength(); i++) {
        Node propNode = props.item(i);
        if (!(propNode instanceof Element)) {
          continue;
        }

        Element prop = (Element)propNode;
        if ("configuration".equals(prop.getTagName())) {
          loadResource(toAddTo, new Resource(prop, name));
          continue;
        }
        if (!"property".equals(prop.getTagName())) {
          LOG.warn("bad conf file: element not <property>");
        }

        String attr = null;
        String value = null;
        boolean finalParameter = false;
        LinkedList<String> source = new LinkedList<>();

        Attr propAttr = prop.getAttributeNode("name");
        if (propAttr != null) {
          attr = StringUtils.weakIntern(propAttr.getValue());
        }
        propAttr = prop.getAttributeNode("value");
        if (propAttr != null) {
          value = StringUtils.weakIntern(propAttr.getValue());
        }
        propAttr = prop.getAttributeNode("final");
        if (propAttr != null) {
          finalParameter = "true".equals(propAttr.getValue());
        }
        propAttr = prop.getAttributeNode("source");
        if (propAttr != null) {
          source.add(StringUtils.weakIntern(propAttr.getValue()));
        }

        NodeList fields = prop.getChildNodes();
        for (int j = 0; j < fields.getLength(); j++) {
          Node fieldNode = fields.item(j);
          if (!(fieldNode instanceof Element)) {
            continue;
          }
          Element field = (Element)fieldNode;
          if ("name".equals(field.getTagName()) && field.hasChildNodes()) {
            attr = StringUtils.weakIntern(
                ((Text)field.getFirstChild()).getData().trim());
          }
          if ("value".equals(field.getTagName()) && field.hasChildNodes()) {
            value = StringUtils.weakIntern(
                ((Text)field.getFirstChild()).getData());
          }
          if ("final".equals(field.getTagName()) && field.hasChildNodes()) {
            finalParameter = "true".equals(((Text)field.getFirstChild()).getData());
          }
          if ("source".equals(field.getTagName()) && field.hasChildNodes()) {
            source.add(StringUtils.weakIntern(
                ((Text)field.getFirstChild()).getData()));
          }
        }
        source.add(name);

        // Ignore this parameter if it has already been marked as 'final'
        if (attr != null) {
          loadProperty(toAddTo, name, attr, value, finalParameter,
              source.toArray(new String[source.size()]));
        }
      }

      if (returnCachedProperties) {
        overlay(propts, toAddTo);
        return new Resource(toAddTo, name);
      }
      return null;
    } catch (IOException | DOMException | SAXException |
        ParserConfigurationException e) {
      LOG.error("error parsing conf " + name, e);
      throw new RuntimeException(e);
    }
  }

  private void overlay(Properties to, Properties from) {
    for (Entry<Object, Object> entry: from.entrySet()) {
      to.put(entry.getKey(), entry.getValue());
    }
  }

  private void loadProperty(Properties prop, String name, String attr,
      String value, boolean finalParameter, String[] source) {
    if (value != null) {
      if (!finalParameters.contains(attr)) {
        prop.setProperty(attr, value);
        if(source != null) {
          updatingResource.put(attr, source);
        }
      } else if (!value.equals(prop.getProperty(attr))) {
        LOG.warn(name+":an attempt to override final parameter: "+attr
            +";  Ignoring.");
      }
    }
    if (finalParameter && attr != null) {
      finalParameters.add(attr);
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Configuration: ");
    if(loadDefaults) {
      toString(DEFAULT_RESOURCES, sb);
      if(resources.size()>0) {
        sb.append(", ");
      }
    }
    toString(resources, sb);
    return sb.toString();
  }

  private <T> void toString(List<T> res, StringBuilder sb) {
    ListIterator<T> i = res.listIterator();
    while (i.hasNext()) {
      if (i.nextIndex() != 0) {
        sb.append(", ");
      }
      sb.append(i.next());
    }
  }

  /**
   * get keys matching the the regex
   * @return a map with matching keys
   */
  public Map<String,String> getValByRegex(String regex) {
    Pattern p = Pattern.compile(regex);

    Map<String,String> result = new HashMap<>();
    Matcher m;

    for(Entry<Object,Object> item: getProps().entrySet()) {
      if (item.getKey() instanceof String &&
          item.getValue() instanceof String) {
        m = p.matcher((String)item.getKey());
        if(m.find()) { // match
          result.put((String) item.getKey(),
              substituteVars(getProps().getProperty((String) item.getKey())));
        }
      }
    }
    return result;
  }
}
