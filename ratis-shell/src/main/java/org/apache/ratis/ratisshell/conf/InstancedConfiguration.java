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
package org.apache.ratis.ratisshell.conf;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.ratis.ratisshell.util.ConfigurationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Ratis shell configuration.
 */
public class InstancedConfiguration implements RatisShellConfiguration {
  private static final Logger LOG = LoggerFactory.getLogger(InstancedConfiguration.class);

  /** Regex string to find "${key}" for variable substitution. */
  private static final String REGEX_STRING = "(\\$\\{([^{}]*)\\})";
  /** Regex to find ${key} for variable substitution. */
  private static final Pattern CONF_REGEX = Pattern.compile(REGEX_STRING);
  /** Source of the truth of all property values (default or customized). */
  private RatisShellProperties properties;

  /**
   * Users should use this API to obtain a configuration for modification before passing to a
   * FileSystem constructor. The default configuration contains all default configuration params
   * and configuration properties modified in the ratis-shell-site.properties file.
   *
   * Example usage:
   *
   * InstancedConfiguration conf = InstancedConfiguration.defaults();
   * conf.set(...);
   * FileSystem fs = FileSystem.Factory.create(conf);
   *
   * WARNING: This API is unstable and may be changed in a future minor release.
   *
   * @return an instanced configuration preset with defaults
   */
  public static InstancedConfiguration defaults() {
    return new InstancedConfiguration(ConfigurationUtils.defaults());
  }

  /**
   * Creates a new instance of {@link InstancedConfiguration}.
   *
   * Application code should use {@link InstancedConfiguration#defaults}.
   *
   * @param properties properties underlying this configuration
   */
  public InstancedConfiguration(RatisShellProperties properties) {
    this.properties = properties;
  }

  /**
   * Creates a new instance of {@link InstancedConfiguration}.
   *
   * Application code should use {@link InstancedConfiguration#defaults}.
   *
   * @param conf configuration to copy
   */
  public InstancedConfiguration(RatisShellConfiguration conf) {
    properties = conf.copyProperties();
  }

  /**
   * @return the properties backing this configuration
   */
  public RatisShellProperties copyProperties() {
    return properties.copy();
  }

  @Override
  public String get(PropertyKey key) {
    String value = properties.get(key);
    if (value == null) {
      // if value or default value is not set in configuration for the given key
      throw new RuntimeException("undefined " + key);
    }
    try {
      value = lookup(value);
    } catch (UnresolvablePropertyException e) {
      throw new RuntimeException("Could not resolve key \""
          + key.getName() + "\": " + e.getMessage(), e);
    }
    return value;
  }

  private boolean isResolvable(PropertyKey key) {
    String val = properties.get(key);
    try {
      // Lookup to resolve any key before simply returning isSet. An exception will be thrown if
      // the key can't be resolved or if a lower level value isn't set.
      lookup(val);
      return true;
    } catch (UnresolvablePropertyException e) {
      return false;
    }
  }

  @Override
  public boolean isSet(PropertyKey key) {
    return properties.isSet(key) && isResolvable(key);
  }

  @Override
  public boolean isSetByUser(PropertyKey key) {
    return properties.isSetByUser(key) && isResolvable(key);
  }

  /**
   * Unsets the value for the appropriate key in the {@link java.util.Properties}.
   * If the {@link PropertyKey} has a default value,
   * it will still be considered set after executing this method.
   *
   * @param key the key to unset
   */
  public void unset(PropertyKey key) {
    Preconditions.checkNotNull(key, "key");
    properties.remove(key);
  }

  /**
   * Merges map of properties into the current properties.
   *
   * @param props map of keys to values
   */
  public void merge(Map<?, ?> props) {
    this.properties.merge(props);
  }

  @Override
  public Set<PropertyKey> keySet() {
    return properties.keySet();
  }

  @Override
  public Set<PropertyKey> userKeySet() {
    return properties.userKeySet();
  }

  @Override
  public int getInt(PropertyKey key) {
    String rawValue = get(key);

    try {
      return Integer.parseInt(rawValue);
    } catch (NumberFormatException e) {
      throw new RuntimeException("key not integer " + rawValue);
    }
  }

  @Override
  public long getLong(PropertyKey key) {
    String rawValue = get(key);

    try {
      return Long.parseLong(rawValue);
    } catch (NumberFormatException e) {
      throw new RuntimeException("key not long " + rawValue);
    }
  }

  @Override
  public double getDouble(PropertyKey key) {
    String rawValue = get(key);

    try {
      return Double.parseDouble(rawValue);
    } catch (NumberFormatException e) {
      throw new RuntimeException("key not double " + rawValue);
    }
  }

  @Override
  public float getFloat(PropertyKey key) {
    String rawValue = get(key);

    try {
      return Float.parseFloat(rawValue);
    } catch (NumberFormatException e) {
      throw new RuntimeException("key not float " + rawValue);
    }
  }

  @Override
  public boolean getBoolean(PropertyKey key) {
    String rawValue = get(key);

    if (rawValue.equalsIgnoreCase("true")) {
      return true;
    } else if (rawValue.equalsIgnoreCase("false")) {
      return false;
    } else {
      throw new RuntimeException("key not boolean " + rawValue);
    }
  }

  @Override
  public List<String> getList(PropertyKey key, String delimiter) {
    Preconditions.checkArgument(delimiter != null,
        "Illegal separator for ratis-shell properties as list");
    String rawValue = get(key);
    return ConfigurationUtils.parseAsList(rawValue, delimiter);
  }

  @Override
  public <T extends Enum<T>> T getEnum(PropertyKey key, Class<T> enumType) {
    String rawValue = get(key).toUpperCase();
    try {
      return Enum.valueOf(enumType, rawValue);
    } catch (IllegalArgumentException e) {
      throw new RuntimeException("key not enum " + rawValue);
    }
  }

  @Override
  public <T> Class<T> getClass(PropertyKey key) {
    String rawValue = get(key);

    try {
      @SuppressWarnings("unchecked")
      Class<T> clazz = (Class<T>) Class.forName(rawValue);
      return clazz;
    } catch (Exception e) {
      LOG.error("requested class could not be loaded: {}", rawValue, e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public Map<String, String> getNestedProperties(PropertyKey prefixKey) {
    Map<String, String> ret = Maps.newHashMap();
    for (Map.Entry<PropertyKey, String> entry: properties.entrySet()) {
      String key = entry.getKey().getName();
      if (prefixKey.isNested(key)) {
        String suffixKey = key.substring(prefixKey.length() + 1);
        ret.put(suffixKey, entry.getValue());
      }
    }
    return ret;
  }

  @Override
  public void validate() {
  }

  /**
   * Lookup key names to handle ${key} stuff.
   *
   * @param base the String to look for
   * @return resolved String value
   */
  private String lookup(final String base) throws UnresolvablePropertyException {
    return lookupRecursively(base, new HashSet<>());
  }

  /**
   * Actual recursive lookup replacement.
   *
   * @param base the string to resolve
   * @param seen strings already seen during this lookup, used to prevent unbound recursion
   * @return the resolved string
   */
  private String lookupRecursively(String base, Set<String> seen)
      throws UnresolvablePropertyException {
    // check argument
    if (base == null) {
      throw new UnresolvablePropertyException("Can't resolve property with null value");
    }

    String resolved = base;
    // Lets find pattern match to ${key}.
    Matcher matcher = CONF_REGEX.matcher(base);
    while (matcher.find()) {
      String match = matcher.group(2).trim();
      if (!seen.add(match)) {
        throw new RuntimeException("KEY_CIRCULAR_DEPENDENCY " + match);
      }
      if (!PropertyKey.isValid(match)) {
        throw new RuntimeException("INVALID_CONFIGURATION_KEY " + match);
      }
      String value = lookupRecursively(properties.get(PropertyKey.fromString(match)), seen);
      seen.remove(match);
      if (value == null) {
        throw new UnresolvablePropertyException("UNDEFINED_CONFIGURATION_KEY");
      }
      resolved = resolved.replaceFirst(REGEX_STRING, Matcher.quoteReplacement(value));
    }
    return resolved;
  }

  private static class UnresolvablePropertyException extends Exception {

    public UnresolvablePropertyException(String msg) {
      super(msg);
    }
  }
}
