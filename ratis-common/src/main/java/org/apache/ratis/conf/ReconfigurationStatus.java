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

import java.util.Map;
import java.util.Objects;

import org.apache.ratis.util.Daemon;
import org.apache.ratis.util.Timestamp;

/** The status of a reconfiguration task. */
public class ReconfigurationStatus {
  private static String quote(String value) {
    return value == null? "<default>": "\"" + value + "\"";
  }

  static String propertyString(String property, String newValue, String oldValue) {
    Objects.requireNonNull(property, "property == null");
    return property + " from " + quote(oldValue) + " to " + quote(newValue);
  }

  /** The change of a configuration property. */
  public static class PropertyChange {
    private final String property;
    private final String newValue;
    private final String oldValue;

    public PropertyChange(String property, String newValue, String oldValue) {
      this.property = property;
      this.newValue = newValue;
      this.oldValue = oldValue;
    }

    /** @return the name of the property being changed. */
    public String getProperty() {
      return property;
    }

    /** @return the new value to be changed to. */
    public String getNewValue() {
      return newValue;
    }

    /** @return the old value of the property. */
    public String getOldValue() {
      return oldValue;
    }

    @Override
    public int hashCode() {
      return property.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      } else if (!(obj instanceof PropertyChange)) {
        return false;
      }
      final PropertyChange that = (PropertyChange)obj;
      return Objects.equals(this.property, that.property)
          && Objects.equals(this.oldValue, that.oldValue)
          && Objects.equals(this.newValue, that.newValue);
    }

    @Override
    public String toString() {
      return propertyString(getProperty(), getNewValue(), getOldValue());
    }
  }

  /** The timestamp when the reconfiguration starts. */
  private final Timestamp startTime;
  /** The timestamp when the reconfiguration completes. */
  private final Timestamp endTime;
  /**
   * A property-change map.
   * For a particular change, if the error is null,
   * it indicates that the change has been applied successfully.
   * Otherwise, it is the error occurred when applying the change.
   */
  private final Map<PropertyChange, Throwable> changes;
  /** The daemon to run the reconfiguration. */
  private final Daemon daemon;

  ReconfigurationStatus(Timestamp startTime, Timestamp endTime, Map<PropertyChange, Throwable> changes, Daemon daemon) {
    this.startTime = startTime;
    this.endTime = endTime;
    this.changes = changes;
    this.daemon = daemon;
  }

  /** @return true iff a reconfiguration task has started (it may either be running or already has finished). */
  public boolean started() {
    return getStartTime() != null;
  }

  /** @return true if the latest reconfiguration task has ended and there are no new active tasks started. */
  public boolean ended() {
    return getEndTime() != null;
  }

  /**
   * @return the start time of the reconfiguration task if the reconfiguration task has been started;
   *         otherwise, return null.
   */
  public Timestamp getStartTime() {
    return startTime;
  }

  /**
   * @return the end time of the reconfiguration task if the reconfiguration task has been ended;
   *         otherwise, return null.
   */
  public Timestamp getEndTime() {
    return endTime;
  }

  /**
   * @return the changes of the reconfiguration task if the reconfiguration task has been ended;
   *         otherwise, return null.
   */
  public Map<PropertyChange, Throwable> getChanges() {
    return changes;
  }

  /**
   * @return the daemon running the reconfiguration task if the task has been started;
   *         otherwise, return null.
   */
  Daemon getDaemon() {
    return daemon;
  }
}
