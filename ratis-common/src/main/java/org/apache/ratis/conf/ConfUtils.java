package org.apache.ratis.conf;

import org.apache.ratis.util.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

public abstract class ConfUtils {
  static Logger LOG = LoggerFactory.getLogger(ConfUtils.class);

  public static int getInt(
      BiFunction<String, Integer, Integer> getInt,
      String key, int defaultValue, Integer min, Integer max) {
    final int value = getInt.apply(key, defaultValue);
    final String s = key + " = " + value;
    LOG.info(s);

    if (min != null && value < min) {
      throw new IllegalArgumentException(s + " < min = " + min);
    }
    if (max != null && value > max) {
      throw new IllegalArgumentException(s + " > max = " + max);
    }
    return value;
  }

  public static String getString(
      BiFunction<String, String, String> getString,
      String key, String defaultValue) {
    final String value = getString.apply(key, defaultValue);
    LOG.info(key + " = " + value);
    return value;
  }

  public static InetSocketAddress getInetSocketAddress(
      BiFunction<String, String, String> getString,
      String key, String defaultValue) {
    return NetUtils.createSocketAddr(getString(getString, key, defaultValue));
  }

  public static void setString(
      BiConsumer<String, String> setString,
      String key, String value) {
    setString.accept(key, value);
    LOG.info("set " + key + " = " + value);
  }
}
