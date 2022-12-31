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

import static org.apache.ratis.conf.ReconfigurationStatus.propertyString;

public class ReconfigurationException extends Exception {
  private static final long serialVersionUID = 1L;

  private final String property;
  private final String newValue;
  private final String oldValue;

  /**
   * Create a new instance of {@link ReconfigurationException}.
   * @param property the property name.
   * @param newValue the new value.
   * @param oldValue the old value.
   * @param cause the cause of this exception.
   */
  public ReconfigurationException(String reason, String property, String newValue, String oldValue, Throwable cause) {
    super("Failed to change property " + propertyString(property, newValue, oldValue) + ": " + reason, cause);
    this.property = property;
    this.newValue = newValue;
    this.oldValue = oldValue;
  }

  /** The same as new ReconfigurationException(reason, property, newValue, oldValue, null). */
  public ReconfigurationException(String reason, String property, String newValue, String oldValue) {
    this(reason, property, newValue, oldValue, null);
  }

  /** @return the property name related to this exception. */
  public String getProperty() {
    return property;
  }

  /** @return the value that the property was supposed to be changed. */
  public String getNewValue() {
    return newValue;
  }

  /** @return the old value of the property. */
  public String getOldValue() {
    return oldValue;
  }
}
