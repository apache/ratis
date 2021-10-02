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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Sets;
import org.apache.ratis.ratisshell.Constants;
import org.apache.ratis.ratisshell.DefaultSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Configuration property keys. This class provides a set of pre-defined property keys.
 */
public final class PropertyKey implements Comparable<PropertyKey> {
  private static final Logger LOG = LoggerFactory.getLogger(PropertyKey.class);

  // The following two maps must be the first to initialize within this file.
  /** A map from default property key's string name to the key. */
  private static final Map<String, PropertyKey> DEFAULT_KEYS_MAP = new ConcurrentHashMap<>();
  /** A cache storing result for template regexp matching results. */
  private static final Cache<String, Boolean> REGEXP_CACHE = CacheBuilder.newBuilder()
      .maximumSize(1024)
      .build();

  /**
   * Builder to create {@link PropertyKey} instances. Note that, <code>Builder.build()</code> will
   * throw exception if there is an existing property built with the same name.
   */
  public static final class Builder {
    private DefaultSupplier mDefaultSupplier;
    private Object mDefaultValue;
    private String mDescription;
    private String mName;

    /**
     * @param name name of the property
     */
    public Builder(String name) {
      mName = name;
    }

    /**
     * @param template template for the property name
     * @param params parameters of the template
     */
    public Builder(Template template, Object... params) {
      mName = String.format(template.mFormat, params);
    }

    /**
     * @param name name for the property
     * @return the updated builder instance
     */
    public Builder setName(String name) {
      mName = name;
      return this;
    }

    /**
     * @param defaultSupplier supplier for the property's default value
     * @return the updated builder instance
     */
    public Builder setDefaultSupplier(DefaultSupplier defaultSupplier) {
      mDefaultSupplier = defaultSupplier;
      return this;
    }

    /**
     * @param supplier supplier for the property's default value
     * @param description description of the default value
     * @return the updated builder instance
     */
    public Builder setDefaultSupplier(Supplier<Object> supplier, String description) {
      mDefaultSupplier = new DefaultSupplier(supplier, description);
      return this;
    }

    /**
     * @param defaultValue the property's default value
     * @return the updated builder instance
     */
    public Builder setDefaultValue(Object defaultValue) {
      mDefaultValue = defaultValue;
      return this;
    }

    /**
     * @param description of the property
     * @return the updated builder instance
     */
    public Builder setDescription(String description) {
      mDescription = description;
      return this;
    }

    /**
     * Creates and registers the property key.
     *
     * @return the created property key instance
     */
    public PropertyKey build() {
      PropertyKey key = buildUnregistered();
      Preconditions.checkState(PropertyKey.register(key), "Cannot register existing key \"%s\"",
          mName);
      return key;
    }

    /**
     * Creates the property key without registering it with default property list.
     *
     * @return the created property key instance
     */
    public PropertyKey buildUnregistered() {
      DefaultSupplier defaultSupplier = mDefaultSupplier;
      if (defaultSupplier == null) {
        String defaultString = String.valueOf(mDefaultValue);
        defaultSupplier = (mDefaultValue == null)
            ? new DefaultSupplier(() -> null, "null")
            : new DefaultSupplier(() -> defaultString, defaultString);
      }

      PropertyKey key = new PropertyKey(mName, mDescription, defaultSupplier);
      return key;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("defaultValue", mDefaultValue)
          .add("description", mDescription)
          .add("name", mName).toString();
    }
  }

  public static final PropertyKey CONF_DIR =
      new Builder(Name.CONF_DIR)
          .setDefaultValue(String.format("${%s}/conf", Name.HOME))
          .setDescription("The directory containing files used to configure ratis-shell.")
          .build();
  public static final PropertyKey TEST_MODE =
      new Builder(Name.TEST_MODE)
          .setDefaultValue(false)
          .setDescription("Flag used only during tests to allow special behavior.")
          .build();
  public static final PropertyKey SITE_CONF_DIR =
      new Builder(Name.SITE_CONF_DIR)
          .setDefaultSupplier(
              () -> String.format("${%s}/,%s/.ratis-shell/,/etc/ratis-shell/",
                  Name.CONF_DIR, System.getProperty("user.home")),
              String.format("${%s}/,${user.home}/.ratis-shell/,/etc/ratis-shell/", Name.CONF_DIR))
          .setDescription(
              String.format("Comma-separated search path for %s.", Constants.SITE_PROPERTIES))
          .build();

  /**
   * A nested class to hold named string constants for their corresponding properties.
   * Used for setting configuration in integration tests.
   */
  public static final class Name {
    public static final String CONF_DIR = "ratis.shell.conf.dir";
    public static final String HOME = "ratis.shell.home";
    public static final String SITE_CONF_DIR = "ratis.shell.site.conf.dir";
    public static final String TEST_MODE = "ratis.shell.test.mode";

    private Name() {} // prevent instantiation
  }

  /**
   * A set of templates to generate the names of parameterized properties given
   * different parameters.
   */
  public enum Template {
    RATIS_SHELL_GROUP_ID("ratis.shell.%s.groupid",
        "ratis\\.shell\\.(\\w+)\\.groupid"),
    RATIS_SHELL_PEER_IDS("ratis.shell.%s.peers",
        "ratis\\.shell\\.(\\w+)\\.peers"),
    ;

    // puts property creators in a nested class to avoid NPE in enum static initialization
    private static class PropertyCreators {
      private static final BiFunction<String, PropertyKey, PropertyKey> DEFAULT_PROPERTY_CREATOR =
          fromBuilder(new Builder(""));

      private static BiFunction<String, PropertyKey, PropertyKey> fromBuilder(Builder builder) {
        return (name, baseProperty) -> builder.setName(name).buildUnregistered();
      }
    }

    private static final String NESTED_GROUP = "nested";
    private final String mFormat;
    private final Pattern mPattern;
    private BiFunction<String, PropertyKey, PropertyKey> mPropertyCreator =
        PropertyCreators.DEFAULT_PROPERTY_CREATOR;

    /**
     * Constructs a property key format.
     *
     * @param format String of this property as formatted string
     * @param re String of this property as regexp
     */
    Template(String format, String re) {
      mFormat = format;
      mPattern = Pattern.compile(re);
    }

    /**
     * Constructs a nested property key format with a function to construct property key given
     * base property key.
     *
     * @param format String of this property as formatted string
     * @param re String of this property as regexp
     * @param propertyCreator a function that creates property key given name and base property key
     *                        (for nested properties only, will be null otherwise)
     */
    Template(String format, String re,
        BiFunction<String, PropertyKey, PropertyKey> propertyCreator) {
      this(format, re);
      mPropertyCreator = propertyCreator;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this).add("format", mFormat).add("pattern", mPattern)
          .toString();
    }

    /**
     * Converts a property key template.
     *
     * @param params ordinal
     * @return corresponding property
     */
    public PropertyKey format(Object... params) {
      return new PropertyKey(String.format(mFormat, params));
    }

    /**
     * @param input the input property key string
     * @return whether the input string matches this template
     */
    public boolean matches(String input) {
      Matcher matcher = mPattern.matcher(input);
      return matcher.matches();
    }

    /**
     * @param input the input property key string
     * @return the matcher matching the template to the string
     */
    public Matcher match(String input) {
      return mPattern.matcher(input);
    }

    /**
     * Gets the property key if the property name matches the template.
     *
     * @param propertyName name of the property
     * @return the property key, or null if the property name does not match the template
     */
    private PropertyKey getPropertyKey(String propertyName) {
      Matcher matcher = match(propertyName);
      if (!matcher.matches()) {
        return null;
      }
      // if the template can extract a nested property, build the new property from the nested one
      String nestedKeyName = null;
      try {
        nestedKeyName = matcher.group(NESTED_GROUP);
      } catch (IllegalArgumentException e) {
        // ignore if group is not found
      }
      PropertyKey nestedProperty = null;
      if (nestedKeyName != null && isValid(nestedKeyName)) {
        nestedProperty = fromString(nestedKeyName);
      }
      return mPropertyCreator.apply(propertyName, nestedProperty);
    }
  }

  /**
   * @param input string of property key
   * @return whether the input is a valid property name
   */
  public static boolean isValid(String input) {
    // Check if input matches any default keys
    if (DEFAULT_KEYS_MAP.containsKey(input)) {
      return true;
    }
    // Regex matching for templates can be expensive when checking properties frequently.
    // Use a cache to store regexp matching results to reduce CPU overhead.
    Boolean result = REGEXP_CACHE.getIfPresent(input);
    if (result != null) {
      return result;
    }
    // Check if input matches any parameterized keys
    result = false;
    for (Template template : Template.values()) {
      if (template.matches(input)) {
        result = true;
        break;
      }
    }
    REGEXP_CACHE.put(input, result);
    return result;
  }

  /**
   * Parses a string and return its corresponding {@link PropertyKey}, throwing exception if no such
   * a property can be found.
   *
   * @param input string of property key
   * @return corresponding property
   */
  public static PropertyKey fromString(String input) {
    // First try to parse it as default key
    PropertyKey key = DEFAULT_KEYS_MAP.get(input);
    if (key != null) {
      return key;
    }

    // Try different templates and see if any template matches
    for (Template template : Template.values()) {
      key = template.getPropertyKey(input);
      if (key != null) {
        return key;
      }
    }

    throw new IllegalArgumentException("Invalid configuration key " + input);
  }

  /**
   * @return all pre-defined property keys
   */
  public static Collection<? extends PropertyKey> defaultKeys() {
    return Sets.newHashSet(DEFAULT_KEYS_MAP.values());
  }

  /** Property name. */
  private final String mName;

  /** Property Key description. */
  private final String mDescription;

  /** Supplies the Property Key default value. */
  private final DefaultSupplier mDefaultSupplier;

  /**
   * @param name String of this property
   * @param description String description of this property key
   * @param defaultSupplier default value supplier
   */
  private PropertyKey(String name, String description, DefaultSupplier defaultSupplier) {
    mName = Preconditions.checkNotNull(name, "name");
    mDescription = Strings.isNullOrEmpty(description) ? "N/A" : description;
    mDefaultSupplier = defaultSupplier;
  }

  /**
   * @param name String of this property
   */
  private PropertyKey(String name) {
    this(name, null, new DefaultSupplier(() -> null, "null"));
  }

  /**
   * Registers the given key to the global key map.
   *
   * @param key th property
   * @return whether the property key is successfully registered
   */
  @VisibleForTesting
  public static boolean register(PropertyKey key) {
    String name = key.getName();

    DEFAULT_KEYS_MAP.put(name, key);
    return true;
  }

  /**
   * Unregisters the given key from the global key map.
   *
   * @param key the property to unregister
   */
  @VisibleForTesting
  public static void unregister(PropertyKey key) {
    String name = key.getName();
    DEFAULT_KEYS_MAP.remove(name);
  }

  /**
   * @param name name of the property
   * @return the registered property key if found, or else create a new one and return
   */
  public static PropertyKey getOrBuildCustom(String name) {
    return DEFAULT_KEYS_MAP.computeIfAbsent(name,
        (key) -> {
          final Builder propertyKeyBuilder = new Builder(key);
          return propertyKeyBuilder.buildUnregistered();
        });
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PropertyKey)) {
      return false;
    }
    PropertyKey that = (PropertyKey) o;
    return Objects.equal(mName, that.mName);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mName);
  }

  @Override
  public String toString() {
    return mName;
  }

  @Override
  public int compareTo(PropertyKey o) {
    return mName.compareTo(o.mName);
  }

  /**
   * @return length of this property key
   */
  public int length() {
    return mName.length();
  }

  /**
   * @param key the name of input key
   * @return if this key is nested inside the given key
   */
  public boolean isNested(String key) {
    return key.length() > length() + 1 && key.startsWith(mName) && key.charAt(length()) == '.';
  }

  /**
   * @return the name of the property
   */
  public String getName() {
    return mName;
  }

  /**
   * @return the description of a property
   */
  public String getDescription() {
    return mDescription;
  }

  /**
   * @return the default value of a property key or null if value not set
   */
  public String getDefaultValue() {
    Object defaultValue = mDefaultSupplier.get();
    return defaultValue == null ? null : defaultValue.toString();
  }
}
