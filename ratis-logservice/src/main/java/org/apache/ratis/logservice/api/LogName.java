package org.apache.ratis.logservice.api;

import static java.util.Objects.requireNonNull;

import java.util.Objects;

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

  // Implementation detail -- we want uses to use the LogName as identifiable, not to
  // know that it's based on the internal String here.
  String getName() {
    return name;
  }

  @Override public boolean equals(Object o) {
    if (!(o instanceof LogName)) {
      return false;
    }
    return Objects.equals(name, ((LogName) o).getName());
  }

  @Override public int hashCode() {
    return name.hashCode();
  }

  @Override public String toString() {
    return "LogName['" + name + "']";
  }

  /**
   * Creates a {@link LogName} given the provided string.
   */
  public static LogName of(String name) {
    // TODO Limit allowed characters in the name?
    return new LogName(name);
  }
}
