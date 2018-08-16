package org.apache.ratis.logservice.api;

import static java.util.Objects.requireNonNull;

/**
 * Identifier to uniquely identify a {@link LogStream}.
 */
public class LogName {
  // It's pretty likely that what uniquely defines a LogStream
  // to change over time. We should account for this by making an
  // API which can naturally evolve.
  private final String name;

  private LogName(String name) {
    this.name = requireNonNull(name);
  }

  String getName() {
    return name;
  }

  /**
   * Creates a {@link LogName} given the provided string.
   */
  public static LogName of(String name) {
    return new LogName(name);
  }
}
