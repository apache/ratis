package org.apache.ratis.logservice.api;

import java.util.Map;
import java.util.Map.Entry;

/**
 * An encapsulation of configuration for a LogStream.
 */
public interface LogStreamConfiguration {

  /**
   * Fetches the value for the given key from the configuration. If there is no entry for
   * the given key, {@code null} is returned.
   *
   * @param key The configuration key
   */
  String get(String key);

  /**
   * Sets the given key and value into this configuration. The configuration key may
   * not be null. A null value removes the key from the configuration.
   *
   * @param key Configuration key, must be non-null
   * @param value Configuration value
   */
  void set(String key, String value);

  /**
   * Removes any entry with the given key from the configuration. If there is no entry
   * for the given key, this method returns without error. The provided key must be
   * non-null.
   *
   * @param key The configuration key, must be non-null
   */
  void remove(String key);

  /**
   * Sets the collection of key-value pairs into the configuration. This is functionally
   * equivalent to calling {@link #set(String, String)} numerous time.
   */
  void setMany(Iterable<Entry<String,String>> entries);

  /**
   * Returns an immutable view over the configuration as a {@code Map}.
   */
  Map<String,String> asMap();
}
