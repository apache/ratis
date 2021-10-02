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
package org.apache.ratis.ratisshell.util;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.ratis.ratisshell.Constants;
import org.apache.ratis.ratisshell.conf.InstancedConfiguration;
import org.apache.ratis.ratisshell.conf.PropertyKey;
import org.apache.ratis.ratisshell.conf.RatisShellConfiguration;
import org.apache.ratis.ratisshell.conf.RatisShellProperties;
import org.apache.ratis.ratisshell.util.io.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Utilities for working with ratis-shell configurations.
 */
public final class ConfigurationUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ConfigurationUtils.class);

  private static volatile RatisShellProperties sDefaultProperties = null;

  private static final Object DEFAULT_PROPERTIES_LOCK = new Object();

  private ConfigurationUtils() {} // prevent instantiation

  /**
   * Loads properties from a resource.
   *
   * @param resource url of the properties file
   * @return a set of properties on success, or null if failed
   */
  public static Properties loadPropertiesFromResource(URL resource) {
    try (InputStream stream = resource.openStream()) {
      return loadProperties(stream);
    } catch (IOException e) {
      LOG.warn("Failed to read properties from {}: {}", resource, e.toString());
      return null;
    }
  }

  /**
   * Loads properties from the given file.
   *
   * @param filePath the absolute path of the file to load properties
   * @return a set of properties on success, or null if failed
   */
  public static Properties loadPropertiesFromFile(String filePath) {
    try (FileInputStream fileInputStream = new FileInputStream(filePath)) {
      return loadProperties(fileInputStream);
    } catch (FileNotFoundException e) {
      return null;
    } catch (IOException e) {
      LOG.warn("Failed to close property input stream from {}: {}", filePath, e.toString());
      return null;
    }
  }

  /**
   * @param stream the stream to read properties from
   * @return a properties object populated from the stream
   */
  public static Properties loadProperties(InputStream stream) {
    Properties properties = new Properties();
    try {
      properties.load(stream);
    } catch (IOException e) {
      LOG.warn("Unable to load properties: {}", e.toString());
      return null;
    }
    return properties;
  }

  /**
   * Searches the given properties file from a list of paths.
   *
   * @param propertiesFile the file to load properties
   * @param confPathList a list of paths to search the propertiesFile
   * @return the site properties file on success search, or null if failed
   */
  public static String searchPropertiesFile(String propertiesFile,
      String[] confPathList) {
    if (propertiesFile == null || confPathList == null) {
      return null;
    }
    for (String path : confPathList) {
      String file = PathUtils.concatPath(path, propertiesFile);
      Properties properties = loadPropertiesFromFile(file);
      if (properties != null) {
        // If a site conf is successfully loaded, stop trying different paths.
        return file;
      }
    }
    return null;
  }

  /**
   * @param value the value or null (value is not set)
   * @return the value or "(no value set)" when the value is not set
   */
  public static String valueAsString(String value) {
    return value == null ? "(no value set)" : value;
  }

  /**
   * Returns an instance of {@link RatisShellConfiguration} with the defaults and values from
   * ratis-shell-site properties.
   *
   * @return the set of properties loaded from the site-properties file
   */
  public static RatisShellProperties defaults() {
    if (sDefaultProperties == null) {
      synchronized (DEFAULT_PROPERTIES_LOCK) { // We don't want multiple threads to reload
        // properties at the same time.
        // Check if properties are still null so we don't reload a second time.
        if (sDefaultProperties == null) {
          reloadProperties();
        }
      }
    }
    return sDefaultProperties.copy();
  }

  /**
   * Reloads site properties from disk.
   */
  public static void reloadProperties() {
    synchronized (DEFAULT_PROPERTIES_LOCK) {
      RatisShellProperties properties = new RatisShellProperties();
      InstancedConfiguration conf = new InstancedConfiguration(properties);
      Properties sysProps = new Properties();
      System.getProperties().stringPropertyNames()
          .forEach(key -> sysProps.setProperty(key, System.getProperty(key)));
      properties.merge(sysProps);

      if (conf.getBoolean(PropertyKey.TEST_MODE)) {
        conf.validate();
        sDefaultProperties = properties;
        return;
      }

      // we are not in test mode, load site properties
      String confPaths = conf.get(PropertyKey.SITE_CONF_DIR);
      String[] confPathList = confPaths.split(",");
      String sitePropertyFile = ConfigurationUtils
          .searchPropertiesFile(Constants.SITE_PROPERTIES, confPathList);
      Properties siteProps = null;
      if (sitePropertyFile != null) {
        siteProps = loadPropertiesFromFile(sitePropertyFile);
      } else {
        URL resource =
            ConfigurationUtils.class.getClassLoader().getResource(Constants.SITE_PROPERTIES);
        if (resource != null) {
          siteProps = loadPropertiesFromResource(resource);
        }
      }
      properties.merge(siteProps);
      conf.validate();
      sDefaultProperties = properties;
    }
  }

  /**
   * Merges the current configuration properties with new properties. If a property exists
   * both in the new and current configuration, the one from the new configuration wins if
   * its priority is higher or equal than the existing one.
   *
   * @param conf the base configuration
   * @param properties the source {@link Properties} to be merged
   * @return a new configuration representing the merged properties
   */
  public static RatisShellConfiguration merge(RatisShellConfiguration conf, Map<?, ?> properties) {
    RatisShellProperties props = conf.copyProperties();
    props.merge(properties);
    return new InstancedConfiguration(props);
  }

  /**
   * Returns the input string as a list, splitting on a specified delimiter.
   *
   * @param value the value to split
   * @param delimiter the delimiter to split the values
   * @return the list of values for input string
   */
  public static List<String> parseAsList(String value, String delimiter) {
    return Lists.newArrayList(Splitter.on(delimiter).trimResults().omitEmptyStrings()
        .split(value));
  }
}
