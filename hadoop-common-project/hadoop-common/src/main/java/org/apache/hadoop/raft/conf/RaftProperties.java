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

package org.apache.hadoop.raft.conf;

import com.google.common.base.Preconditions;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.util.StringUtils;
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

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.ref.WeakReference;
import java.net.InetSocketAddress;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/** 
 * Provides access to configuration parameters. The current implementation is a
 * simplified version of hadoop's Configuration.
 */
public class RaftProperties {
  private static final Logger LOG = LoggerFactory.getLogger(RaftProperties.class);

  private static class Resource {
    private final Object resource;
    private final String name;

    public Resource(Object resource) {
      this(resource, resource.toString());
    }

    public Resource(Object resource, String name) {
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
  private ArrayList<Resource> resources = new ArrayList<>();

  /**
   * The value reported as the setting resource when a key is set
   * by code rather than a file resource by dumpConfiguration.
   */
  static final String UNKNOWN_RESOURCE = "Unknown";

  /**
   * List of configuration parameters marked <b>final</b>.
   */
  private Set<String> finalParameters = Collections.newSetFromMap(
      new ConcurrentHashMap<String, Boolean>());

  private boolean loadDefaults = true;

  /**
   * Configuration objects
   */
  private static final WeakHashMap<RaftProperties, Object> REGISTRY = new WeakHashMap<>();

  /**
   * List of default Resources. Resources are loaded in the order of the list
   * entries
   */
  private static final CopyOnWriteArrayList<String> defaultResources =
    new CopyOnWriteArrayList<>();

  /**
   * Stores the mapping of key to the resource which modifies or loads
   * the key most recently
   */
  private Map<String, String[]> updatingResource;

  private Properties properties;
  private Properties overlay;
  private ClassLoader classLoader;
  {
    classLoader = Thread.currentThread().getContextClassLoader();
    if (classLoader == null) {
      classLoader = RaftProperties.class.getClassLoader();
    }
  }

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
    updatingResource = new ConcurrentHashMap<>();
    synchronized(RaftProperties.class) {
      REGISTRY.put(this, null);
    }
  }

  /**
   * Add a default resource. Resources are loaded in the order of the resources
   * added.
   * @param name file name. File should be present in the classpath.
   */
  public static synchronized void addDefaultResource(String name) {
    if(!defaultResources.contains(name)) {
      defaultResources.add(name);
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
    Preconditions.checkArgument(name != null, "Property name must not be null");
    Preconditions.checkArgument(value != null,
        "The value of property " + name + " must not be null");
    name = name.trim();
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
    if (valueString == null)
      return defaultValue;
    String hexString = getHexDigits(valueString);
    if (hexString != null) {
      return Integer.parseInt(hexString, 16);
    }
    return Integer.parseInt(valueString);
  }

  /**
   * Get the value of the <code>name</code> property as a set of comma-delimited
   * <code>int</code> values.
   *
   * If no such property exists, an empty array is returned.
   *
   * @param name property name
   * @return property value interpreted as an array of comma-delimited
   *         <code>int</code> values
   */
  public int[] getInts(String name) {
    String[] strings = getTrimmedStrings(name);
    int[] ints = new int[strings.length];
    for (int i = 0; i < strings.length; i++) {
      ints[i] = Integer.parseInt(strings[i]);
    }
    return ints;
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
    if (valueString == null)
      return defaultValue;
    String hexString = getHexDigits(valueString);
    if (hexString != null) {
      return Long.parseLong(hexString, 16);
    }
    return Long.parseLong(valueString);
  }

  /**
   * Get the value of the <code>name</code> property as a <code>long</code> or
   * human readable format. If no such property exists, the provided default
   * value is returned, or if the specified value is not a valid
   * <code>long</code> or human readable format, then an error is thrown. You
   * can use the following suffix (case insensitive): k(kilo), m(mega), g(giga),
   * t(tera), p(peta), e(exa)
   *
   * @param name property name.
   * @param defaultValue default value.
   * @throws NumberFormatException when the value is invalid
   * @return property value as a <code>long</code>,
   *         or <code>defaultValue</code>.
   */
  public long getLongBytes(String name, long defaultValue) {
    String valueString = getTrimmed(name);
    if (valueString == null)
      return defaultValue;
    return StringUtils.TraditionalBinaryPrefix.string2long(valueString);
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
   * Get the value of the <code>name</code> property as a <code>float</code>.
   * If no such property exists, the provided default value is returned,
   * or if the specified value is not a valid <code>float</code>,
   * then an error is thrown.
   *
   * @param name property name.
   * @param defaultValue default value.
   * @throws NumberFormatException when the value is invalid
   * @return property value as a <code>float</code>,
   *         or <code>defaultValue</code>.
   */
  public float getFloat(String name, float defaultValue) {
    String valueString = getTrimmed(name);
    if (valueString == null)
      return defaultValue;
    return Float.parseFloat(valueString);
  }

  /**
   * Set the value of the <code>name</code> property to a <code>float</code>.
   *
   * @param name property name.
   * @param value property value.
   */
  public void setFloat(String name, float value) {
    set(name,Float.toString(value));
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
    if (valueString == null)
      return defaultValue;
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
    if (null == valueString || valueString.isEmpty()) {
      return defaultValue;
    }

    if (StringUtils.equalsIgnoreCase("true", valueString)) {
      return true;
    } else if (StringUtils.equalsIgnoreCase("false", valueString)) {
      return false;
    } else {
      return defaultValue;
    }
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

  enum ParsedTimeDuration {
    NS {
      TimeUnit unit() { return TimeUnit.NANOSECONDS; }
      String suffix() { return "ns"; }
    },
    US {
      TimeUnit unit() { return TimeUnit.MICROSECONDS; }
      String suffix() { return "us"; }
    },
    MS {
      TimeUnit unit() { return TimeUnit.MILLISECONDS; }
      String suffix() { return "ms"; }
    },
    S {
      TimeUnit unit() { return TimeUnit.SECONDS; }
      String suffix() { return "s"; }
    },
    M {
      TimeUnit unit() { return TimeUnit.MINUTES; }
      String suffix() { return "m"; }
    },
    H {
      TimeUnit unit() { return TimeUnit.HOURS; }
      String suffix() { return "h"; }
    },
    D {
      TimeUnit unit() { return TimeUnit.DAYS; }
      String suffix() { return "d"; }
    };
    abstract TimeUnit unit();
    abstract String suffix();
    static ParsedTimeDuration unitFor(String s) {
      for (ParsedTimeDuration ptd : values()) {
        // iteration order is in decl order, so SECONDS matched last
        if (s.endsWith(ptd.suffix())) {
          return ptd;
        }
      }
      return null;
    }
    static ParsedTimeDuration unitFor(TimeUnit unit) {
      for (ParsedTimeDuration ptd : values()) {
        if (ptd.unit() == unit) {
          return ptd;
        }
      }
      return null;
    }
  }

  /**
   * Set the value of <code>name</code> to the given time duration. This
   * is equivalent to <code>set(&lt;name&gt;, value + &lt;time suffix&gt;)</code>.
   * @param name Property name
   * @param value Time duration
   * @param unit Unit of time
   */
  public void setTimeDuration(String name, long value, TimeUnit unit) {
    set(name, value + ParsedTimeDuration.unitFor(unit).suffix());
  }

  /**
   * Return time duration in the given time unit. Valid units are encoded in
   * properties as suffixes: nanoseconds (ns), microseconds (us), milliseconds
   * (ms), seconds (s), minutes (m), hours (h), and days (d).
   * @param name Property name
   * @param defaultValue Value returned if no mapping exists.
   * @param unit Unit to convert the stored property, if it exists.
   * @throws NumberFormatException If the property stripped of its unit is not
   *         a number
   */
  public long getTimeDuration(String name, long defaultValue, TimeUnit unit) {
    String vStr = get(name);
    if (null == vStr) {
      return defaultValue;
    }
    vStr = vStr.trim();
    return getTimeDurationHelper(name, vStr, unit);
  }

  private long getTimeDurationHelper(String name, String vStr, TimeUnit unit) {
    ParsedTimeDuration vUnit = ParsedTimeDuration.unitFor(vStr);
    if (null == vUnit) {
      LOG.warn("No unit for " + name + "(" + vStr + ") assuming " + unit);
      vUnit = ParsedTimeDuration.unitFor(unit);
    } else {
      vStr = vStr.substring(0, vStr.lastIndexOf(vUnit.suffix()));
    }
    return unit.convert(Long.parseLong(vStr), vUnit.unit());
  }

  public long[] getTimeDurations(String name, TimeUnit unit) {
    String[] strings = getTrimmedStrings(name);
    long[] durations = new long[strings.length];
    for (int i = 0; i < strings.length; i++) {
      durations[i] = getTimeDurationHelper(name, strings[i], unit);
    }
    return durations;
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
   * A class that represents a set of positive integer ranges. It parses
   * strings of the form: "2-3,5,7-" where ranges are separated by comma and
   * the lower/upper bounds are separated by dash. Either the lower or upper
   * bound may be omitted meaning all values up to or over. So the string
   * above means 2, 3, 5, and 7, 8, 9, ...
   */
  public static class IntegerRanges implements Iterable<Integer>{
    private static class Range {
      int start;
      int end;
    }

    private static class RangeNumberIterator implements Iterator<Integer> {
      Iterator<Range> internal;
      int at;
      int end;

      public RangeNumberIterator(List<Range> ranges) {
        if (ranges != null) {
          internal = ranges.iterator();
        }
        at = -1;
        end = -2;
      }

      @Override
      public boolean hasNext() {
        if (at <= end) {
          return true;
        } else if (internal != null){
          return internal.hasNext();
        }
        return false;
      }

      @Override
      public Integer next() {
        if (at <= end) {
          at++;
          return at - 1;
        } else if (internal != null){
          Range found = internal.next();
          if (found != null) {
            at = found.start;
            end = found.end;
            at++;
            return at - 1;
          }
        }
        return null;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    }

    List<Range> ranges = new ArrayList<>();

    public IntegerRanges() {
    }

    public IntegerRanges(String newValue) {
      StringTokenizer itr = new StringTokenizer(newValue, ",");
      while (itr.hasMoreTokens()) {
        String rng = itr.nextToken().trim();
        String[] parts = rng.split("-", 3);
        if (parts.length < 1 || parts.length > 2) {
          throw new IllegalArgumentException("integer range badly formed: " +
                                             rng);
        }
        Range r = new Range();
        r.start = convertToInt(parts[0], 0);
        if (parts.length == 2) {
          r.end = convertToInt(parts[1], Integer.MAX_VALUE);
        } else {
          r.end = r.start;
        }
        if (r.start > r.end) {
          throw new IllegalArgumentException("IntegerRange from " + r.start +
                                             " to " + r.end + " is invalid");
        }
        ranges.add(r);
      }
    }

    /**
     * Convert a string to an int treating empty strings as the default value.
     * @param value the string value
     * @param defaultValue the value for if the string is empty
     * @return the desired integer
     */
    private static int convertToInt(String value, int defaultValue) {
      String trim = value.trim();
      if (trim.length() == 0) {
        return defaultValue;
      }
      return Integer.parseInt(trim);
    }

    /**
     * Is the given value in the set of ranges
     * @param value the value to check
     * @return is the value in the ranges?
     */
    public boolean isIncluded(int value) {
      for(Range r: ranges) {
        if (r.start <= value && value <= r.end) {
          return true;
        }
      }
      return false;
    }

    /**
     * @return true if there are no values in this range, else false.
     */
    public boolean isEmpty() {
      return ranges == null || ranges.isEmpty();
    }

    @Override
    public String toString() {
      StringBuilder result = new StringBuilder();
      boolean first = true;
      for(Range r: ranges) {
        if (first) {
          first = false;
        } else {
          result.append(',');
        }
        result.append(r.start);
        result.append('-');
        result.append(r.end);
      }
      return result.toString();
    }

    @Override
    public Iterator<Integer> iterator() {
      return new RangeNumberIterator(ranges);
    }

  }

  /**
   * Parse the given attribute as a set of integer ranges
   * @param name the attribute name
   * @param defaultValue the default value if it is not set
   * @return a new set of ranges from the configured value
   */
  public IntegerRanges getRange(String name, String defaultValue) {
    return new IntegerRanges(get(name, defaultValue));
  }

  /**
   * Get the comma delimited values of the <code>name</code> property as
   * a collection of <code>String</code>s.
   * If no such property is specified then empty collection is returned.
   * <p>
   * This is an optimized version of {@link #getStrings(String)}
   *
   * @param name property name.
   * @return property value as a collection of <code>String</code>s.
   */
  public Collection<String> getStringCollection(String name) {
    String valueString = get(name);
    return StringUtils.getStringCollection(valueString);
  }

  /**
   * Get the comma delimited values of the <code>name</code> property as
   * an array of <code>String</code>s.
   * If no such property is specified then <code>null</code> is returned.
   *
   * @param name property name.
   * @return property value as an array of <code>String</code>s,
   *         or <code>null</code>.
   */
  public String[] getStrings(String name) {
    String valueString = get(name);
    return StringUtils.getStrings(valueString);
  }

  /**
   * Get the comma delimited values of the <code>name</code> property as
   * an array of <code>String</code>s.
   * If no such property is specified then default value is returned.
   *
   * @param name property name.
   * @param defaultValue The default value
   * @return property value as an array of <code>String</code>s,
   *         or default value.
   */
  public String[] getStrings(String name, String... defaultValue) {
    String valueString = get(name);
    if (valueString == null) {
      return defaultValue;
    } else {
      return StringUtils.getStrings(valueString);
    }
  }

  /**
   * Get the comma delimited values of the <code>name</code> property as
   * a collection of <code>String</code>s, trimmed of the leading and trailing whitespace.
   * If no such property is specified then empty <code>Collection</code> is returned.
   *
   * @param name property name.
   * @return property value as a collection of <code>String</code>s, or empty <code>Collection</code>
   */
  public Collection<String> getTrimmedStringCollection(String name) {
    String valueString = get(name);
    if (null == valueString) {
      return new ArrayList<>();
    }
    return StringUtils.getTrimmedStringCollection(valueString);
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
   * Get the comma delimited values of the <code>name</code> property as
   * an array of <code>String</code>s, trimmed of the leading and trailing whitespace.
   * If no such property is specified then default value is returned.
   *
   * @param name property name.
   * @param defaultValue The default value
   * @return property value as an array of trimmed <code>String</code>s,
   *         or default value.
   */
  public String[] getTrimmedStrings(String name, String... defaultValue) {
    String valueString = get(name);
    if (null == valueString) {
      return defaultValue;
    } else {
      return StringUtils.getTrimmedStrings(valueString);
    }
  }

  /**
   * Set the array of string values for the <code>name</code> property as
   * as comma delimited values.
   *
   * @param name property name.
   * @param values The values
   */
  public void setStrings(String name, String... values) {
    set(name, StringUtils.arrayToString(values));
  }

  /**
   * Get the socket address for <code>hostProperty</code> as a
   * <code>InetSocketAddress</code>. If <code>hostProperty</code> is
   * <code>null</code>, <code>addressProperty</code> will be used. This
   * is useful for cases where we want to differentiate between host
   * bind address and address clients should use to establish connection.
   *
   * @param hostProperty bind host property name.
   * @param addressProperty address property name.
   * @param defaultAddressValue the default value
   * @param defaultPort the default port
   * @return InetSocketAddress
   */
  public InetSocketAddress getSocketAddr(
      String hostProperty,
      String addressProperty,
      String defaultAddressValue,
      int defaultPort) {

    InetSocketAddress bindAddr = getSocketAddr(
      addressProperty, defaultAddressValue, defaultPort);

    final String host = get(hostProperty);

    if (host == null || host.isEmpty()) {
      return bindAddr;
    }

    return NetUtils.createSocketAddr(
        host, bindAddr.getPort(), hostProperty);
  }

  /**
   * Get the socket address for <code>name</code> property as a
   * <code>InetSocketAddress</code>.
   * @param name property name.
   * @param defaultAddress the default value
   * @param defaultPort the default port
   * @return InetSocketAddress
   */
  public InetSocketAddress getSocketAddr(
      String name, String defaultAddress, int defaultPort) {
    final String address = getTrimmed(name, defaultAddress);
    return NetUtils.createSocketAddr(address, defaultPort, name);
  }

  /**
   * Set the socket address for the <code>name</code> property as
   * a <code>host:port</code>.
   */
  public void setSocketAddr(String name, InetSocketAddress addr) {
    set(name, NetUtils.getHostPortString(addr));
  }

  /**
   * Set the socket address a client can use to connect for the
   * <code>name</code> property as a <code>host:port</code>.  The wildcard
   * address is replaced with the local host's address. If the host and address
   * properties are configured the host component of the address will be combined
   * with the port component of the addr to generate the address.  This is to allow
   * optional control over which host name is used in multi-home bind-host
   * cases where a host can have multiple names
   * @param hostProperty the bind-host configuration name
   * @param addressProperty the service address configuration name
   * @param defaultAddressValue the service default address configuration value
   * @param addr InetSocketAddress of the service listener
   * @return InetSocketAddress for clients to connect
   */
  public InetSocketAddress updateConnectAddr(
      String hostProperty,
      String addressProperty,
      String defaultAddressValue,
      InetSocketAddress addr) {

    final String host = get(hostProperty);
    final String connectHostPort = getTrimmed(addressProperty, defaultAddressValue);

    if (host == null || host.isEmpty() || connectHostPort == null || connectHostPort.isEmpty()) {
      //not our case, fall back to original logic
      return updateConnectAddr(addressProperty, addr);
    }

    final String connectHost = connectHostPort.split(":")[0];
    // Create connect address using client address hostname and server port.
    return updateConnectAddr(addressProperty, NetUtils.createSocketAddrForHost(
        connectHost, addr.getPort()));
  }

  /**
   * Set the socket address a client can use to connect for the
   * <code>name</code> property as a <code>host:port</code>.  The wildcard
   * address is replaced with the local host's address.
   * @param name property name.
   * @param addr InetSocketAddress of a listener to store in the given property
   * @return InetSocketAddress for clients to connect
   */
  public InetSocketAddress updateConnectAddr(String name,
                                             InetSocketAddress addr) {
    final InetSocketAddress connectAddr = NetUtils.getConnectAddress(addr);
    setSocketAddr(name, connectAddr);
    return connectAddr;
  }

  /**
   * Load a class by name.
   *
   * @param name the class name.
   * @return the class object.
   * @throws ClassNotFoundException if the class is not found.
   */
  public Class<?> getClassByName(String name) throws ClassNotFoundException {
    Class<?> ret = getClassByNameOrNull(name);
    if (ret == null) {
      throw new ClassNotFoundException("Class " + name + " not found");
    }
    return ret;
  }

  private static final Map<ClassLoader, Map<String, WeakReference<Class<?>>>>
      CACHE_CLASSES = new WeakHashMap<>();

  /**
   * Sentinel value to store negative cache results in {@link #CACHE_CLASSES}.
   */
  private static final Class<?> NEGATIVE_CACHE_SENTINEL =
      NegativeCacheSentinel.class;

  /**
   * Load a class by name, returning null rather than throwing an exception
   * if it couldn't be loaded. This is to avoid the overhead of creating
   * an exception.
   *
   * @param name the class name
   * @return the class object, or null if it could not be found.
   */
  public Class<?> getClassByNameOrNull(String name) {
    Map<String, WeakReference<Class<?>>> map;

    synchronized (CACHE_CLASSES) {
      map = CACHE_CLASSES.get(classLoader);
      if (map == null) {
        map = Collections.synchronizedMap(
          new WeakHashMap<String, WeakReference<Class<?>>>());
        CACHE_CLASSES.put(classLoader, map);
      }
    }

    Class<?> clazz = null;
    WeakReference<Class<?>> ref = map.get(name);
    if (ref != null) {
       clazz = ref.get();
    }

    if (clazz == null) {
      try {
        clazz = Class.forName(name, true, classLoader);
      } catch (ClassNotFoundException e) {
        // Leave a marker that the class isn't found
        map.put(name, new WeakReference<>(NEGATIVE_CACHE_SENTINEL));
        return null;
      }
      // two putters can race here, but they'll put the same class
      map.put(name, new WeakReference<>(clazz));
      return clazz;
    } else if (clazz == NEGATIVE_CACHE_SENTINEL) {
      return null; // not found
    } else {
      // cache hit
      return clazz;
    }
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
        classes[i] = getClassByName(classnames[i]);
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
    if (valueString == null)
      return defaultValue;
    try {
      return getClassByName(valueString);
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
  public <U> Class<? extends U> getClass(String name,
                                         Class<? extends U> defaultValue,
                                         Class<U> xface) {
    try {
      Class<?> theClass = getClass(name, defaultValue);
      if (theClass != null && !xface.isAssignableFrom(theClass))
        throw new RuntimeException(theClass+" not "+xface.getName());
      else if (theClass != null)
        return theClass.asSubclass(xface);
      else
        return null;
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
    if (!xface.isAssignableFrom(theClass))
      throw new RuntimeException(theClass+" not "+xface.getName());
    set(name, theClass.getName());
  }

  /**
   * Get the {@link URL} for the named resource.
   *
   * @param name resource name.
   * @return the url for the named resource.
   */
  public URL getResource(String name) {
    return classLoader.getResource(name);
  }

  protected synchronized Properties getProps() {
    if (properties == null) {
      properties = new Properties();
      Map<String, String[]> backup =
          new ConcurrentHashMap<>(updatingResource);
      loadResources(properties, resources);

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

  private void loadResources(Properties properties,
                             ArrayList<Resource> resources) {
    if(loadDefaults) {
      for (String resource : defaultResources) {
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

  private Resource loadResource(Properties properties, Resource wrapper) {
    String name = UNKNOWN_RESOURCE;
    try {
      Object resource = wrapper.getResource();
      name = wrapper.getName();

      DocumentBuilderFactory docBuilderFactory
        = DocumentBuilderFactory.newInstance();
      //ignore all comments inside the xml file
      docBuilderFactory.setIgnoringComments(true);

      //allow includes in the xml file
      docBuilderFactory.setNamespaceAware(true);
      try {
          docBuilderFactory.setXIncludeAware(true);
      } catch (UnsupportedOperationException e) {
        LOG.error("Failed to set setXIncludeAware(true) for parser "
                + docBuilderFactory
                + ":" + e,
                e);
      }
      DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
      Document doc = null;
      Element root = null;
      boolean returnCachedProperties = false;

      if (resource instanceof URL) { // an URL resource
        doc = parse(builder, (URL) resource);
      } else if (resource instanceof String) { // a CLASSPATH resource
        URL url = getResource((String) resource);
        doc = parse(builder, url);
      } else if (resource instanceof InputStream) {
        doc = parse(builder, (InputStream) resource, null);
        returnCachedProperties = true;
      } else if (resource instanceof Properties) {
        overlay(properties, (Properties) resource);
      } else if (resource instanceof Element) {
        root = (Element) resource;
      }

      if (root == null) {
        if (doc == null) {
          return null;
        }
        root = doc.getDocumentElement();
      }
      Properties toAddTo = properties;
      if(returnCachedProperties) {
        toAddTo = new Properties();
      }
      if (!"configuration".equals(root.getTagName())) {
        LOG.error("bad conf file: top-level element not <configuration>");
      }
      NodeList props = root.getChildNodes();
      for (int i = 0; i < props.getLength(); i++) {
        Node propNode = props.item(i);
        if (!(propNode instanceof Element))
          continue;
        Element prop = (Element)propNode;
        if ("configuration".equals(prop.getTagName())) {
          loadResource(toAddTo, new Resource(prop, name));
          continue;
        }
        if (!"property".equals(prop.getTagName()))
          LOG.warn("bad conf file: element not <property>");

        String attr = null;
        String value = null;
        boolean finalParameter = false;
        LinkedList<String> source = new LinkedList<>();

        Attr propAttr = prop.getAttributeNode("name");
        if (propAttr != null)
          attr = StringInterner.weakIntern(propAttr.getValue());
        propAttr = prop.getAttributeNode("value");
        if (propAttr != null)
          value = StringInterner.weakIntern(propAttr.getValue());
        propAttr = prop.getAttributeNode("final");
        if (propAttr != null)
          finalParameter = "true".equals(propAttr.getValue());
        propAttr = prop.getAttributeNode("source");
        if (propAttr != null)
          source.add(StringInterner.weakIntern(propAttr.getValue()));

        NodeList fields = prop.getChildNodes();
        for (int j = 0; j < fields.getLength(); j++) {
          Node fieldNode = fields.item(j);
          if (!(fieldNode instanceof Element))
            continue;
          Element field = (Element)fieldNode;
          if ("name".equals(field.getTagName()) && field.hasChildNodes())
            attr = StringInterner.weakIntern(
                ((Text)field.getFirstChild()).getData().trim());
          if ("value".equals(field.getTagName()) && field.hasChildNodes())
            value = StringInterner.weakIntern(
                ((Text)field.getFirstChild()).getData());
          if ("final".equals(field.getTagName()) && field.hasChildNodes())
            finalParameter = "true".equals(((Text)field.getFirstChild()).getData());
          if ("source".equals(field.getTagName()) && field.hasChildNodes())
            source.add(StringInterner.weakIntern(
                ((Text)field.getFirstChild()).getData()));
        }
        source.add(name);

        // Ignore this parameter if it has already been marked as 'final'
        if (attr != null) {
          loadProperty(toAddTo, name, attr, value, finalParameter,
              source.toArray(new String[source.size()]));
        }
      }

      if (returnCachedProperties) {
        overlay(properties, toAddTo);
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

  private void loadProperty(Properties properties, String name, String attr,
      String value, boolean finalParameter, String[] source) {
    if (value != null) {
      if (!finalParameters.contains(attr)) {
        properties.setProperty(attr, value);
        if(source != null) {
          updatingResource.put(attr, source);
        }
      } else if (!value.equals(properties.getProperty(attr))) {
        LOG.warn(name+":an attempt to override final parameter: "+attr
            +";  Ignoring.");
      }
    }
    if (finalParameter && attr != null) {
      finalParameters.add(attr);
    }
  }

  /**
   * Write out the non-default properties in this configuration to the given
   * {@link OutputStream} using UTF-8 encoding.
   *
   * @param out the output stream to write to.
   */
  public void writeXml(OutputStream out) throws IOException {
    writeXml(new OutputStreamWriter(out, "UTF-8"));
  }

  /**
   * Write out the non-default properties in this configuration to the given
   * {@link Writer}.
   *
   * @param out the writer to write to.
   */
  public void writeXml(Writer out) throws IOException {
    Document doc = asXmlDocument();

    try {
      DOMSource source = new DOMSource(doc);
      StreamResult result = new StreamResult(out);
      TransformerFactory transFactory = TransformerFactory.newInstance();
      Transformer transformer = transFactory.newTransformer();

      // Important to not hold Configuration log while writing result, since
      // 'out' may be an HDFS stream which needs to lock this configuration
      // from another thread.
      transformer.transform(source, result);
    } catch (TransformerException te) {
      throw new IOException(te);
    }
  }

  /**
   * Return the XML DOM corresponding to this Configuration.
   */
  private synchronized Document asXmlDocument() throws IOException {
    Document doc;
    try {
      doc =
        DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
    } catch (ParserConfigurationException pe) {
      throw new IOException(pe);
    }
    Element conf = doc.createElement("configuration");
    doc.appendChild(conf);
    conf.appendChild(doc.createTextNode("\n"));
    for (Enumeration<Object> e = properties.keys(); e.hasMoreElements();) {
      String name = (String)e.nextElement();
      Object object = properties.get(name);
      String value;
      if (object instanceof String) {
        value = (String) object;
      }else {
        continue;
      }
      Element propNode = doc.createElement("property");
      conf.appendChild(propNode);

      Element nameNode = doc.createElement("name");
      nameNode.appendChild(doc.createTextNode(name));
      propNode.appendChild(nameNode);

      Element valueNode = doc.createElement("value");
      valueNode.appendChild(doc.createTextNode(value));
      propNode.appendChild(valueNode);

      if (updatingResource != null) {
        String[] sources = updatingResource.get(name);
        if(sources != null) {
          for(String s : sources) {
            Element sourceNode = doc.createElement("source");
            sourceNode.appendChild(doc.createTextNode(s));
            propNode.appendChild(sourceNode);
          }
        }
      }

      conf.appendChild(doc.createTextNode("\n"));
    }
    return doc;
  }

  /**
   * Get the {@link ClassLoader} for this job.
   *
   * @return the correct class loader.
   */
  public ClassLoader getClassLoader() {
    return classLoader;
  }

  /**
   * Set the class loader that will be used to load the various objects.
   *
   * @param classLoader the new class loader.
   */
  public void setClassLoader(ClassLoader classLoader) {
    this.classLoader = classLoader;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Configuration: ");
    if(loadDefaults) {
      toString(defaultResources, sb);
      if(resources.size()>0) {
        sb.append(", ");
      }
    }
    toString(resources, sb);
    return sb.toString();
  }

  private <T> void toString(List<T> resources, StringBuilder sb) {
    ListIterator<T> i = resources.listIterator();
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

  /**
   * A unique class which is used as a sentinel value in the caching
   * for getClassByName. {@link RaftProperties#getClassByNameOrNull(String)}
   */
  private static abstract class NegativeCacheSentinel {}
}
